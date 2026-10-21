use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn fixture(
    kind: &DataType,
    rows: usize,
    width: usize,
    nulls: bool,
    op: i64,
) -> (RecordBatch, ArrayRef) {
    let samples = (0..=rows)
        .map(|row| {
            (!nulls || row % 7 != 0).then(|| {
                (0..width)
                    .map(|i| (!nulls || i % 5 != 0).then_some(((row + i) % 9) as i64 - 4))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let needles = (0..=rows)
        .map(|row| (!nulls || row % 3 != 0).then_some((row % 9) as i64 - 4))
        .collect::<Vec<_>>();
    let expected = samples
        .iter()
        .zip(&needles)
        .map(|(array, needle)| {
            array.as_ref().map(|a| {
                let mut a = a.clone();
                match op {
                    182 => {
                        a.push(*needle);
                    }
                    183 => {
                        a.insert(0, *needle);
                    }
                    184 => {
                        a.retain(|v| v != needle);
                    }
                    185 => {
                        a.extend_from_slice(array.as_ref().unwrap());
                    }
                    186 => {
                        let mut seen = std::collections::HashSet::new();
                        a.retain(|v| seen.insert(*v));
                    }
                    187 => {
                        a.clear();
                    }
                    188 => {
                        let mut seen = std::collections::HashSet::new();
                        a.retain(|v| seen.insert(*v));
                    }
                    _ => unreachable!(),
                }
                a
            })
        })
        .collect::<Vec<_>>();
    let ty = DataType::List(Arc::new(Field::new_list_field(kind.clone(), true)));
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(samples).slice(1, rows);
    let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(expected).slice(1, rows);
    let lists = arrow::compute::cast(&lists, &ty).unwrap();
    let needles = arrow::compute::cast(&Int64Array::from(needles).slice(1, rows), kind).unwrap();
    (
        RecordBatch::try_from_iter([("a", lists), ("n", needles)]).unwrap(),
        arrow::compute::cast(&expected, &ty).unwrap(),
    )
}
fn run(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("array_edits");
    for rows in [0, 16, 1024] {
        for width in [0, 8, 64] {
            for nulls in [false, true] {
                for kind in [
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Decimal128(38, 9),
                ] {
                    for op in [182, 183, 184, 185, 186, 187, 188] {
                        let (batch, expected) = fixture(&kind, rows, width, nulls, op);
                        let mut calc = CalcProgram::new(
                            vec![6, 0, 0],
                            vec![op, 0, if op >= 185 { 0 } else { 1 }],
                            vec![2, 0, 0],
                            vec![],
                            vec![],
                            vec![],
                        );
                        let name = match op {
                            182 => "append",
                            183 => "prepend",
                            184 => "remove",
                            185 => "concat",
                            186 => "union",
                            187 => "except",
                            188 => "intersect",
                            _ => unreachable!(),
                        };
                        let label = format!("{name}/{kind}/{rows}/width={width}/nulls={nulls}");
                        assert_eq!(
                            calc.run(batch.clone()).column(0).as_ref(),
                            expected.as_ref(),
                            "{label}"
                        );
                        let (output, allocations) = measure(|| calc.run(batch.clone()));
                        report(&label, batch.columns(), output.columns(), allocations);
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter(|| std::hint::black_box(calc.run(batch.clone())))
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, run);
criterion_main!(benches);
