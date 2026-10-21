use arrow::array::{ArrayRef, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn run(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("array_sort");
    for rows in [0, 16, 1024] {
        for width in [0, 8, 31, 64] {
            for nulls in [false, true] {
                for kind in [
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Decimal128(38, 9),
                ] {
                    for ascending in [false, true] {
                        for null_first in [false, true] {
                            let samples = (0..=rows)
                                .map(|row| {
                                    (!nulls || row % 7 != 0).then(|| {
                                        (0..width)
                                            .map(|i| {
                                                (!nulls || i % 9 != 0).then_some(
                                                    ((row + 31 * i) % width.max(1)) as i64 - 32,
                                                )
                                            })
                                            .collect::<Vec<_>>()
                                    })
                                })
                                .collect::<Vec<_>>();
                            let expected = samples
                                .iter()
                                .map(|a| {
                                    a.as_ref().map(|a| {
                                        let mut a = a.clone();
                                        a.sort_by(|l, r| match (l, r) {
                                            (None, None) => std::cmp::Ordering::Equal,
                                            (None, _) => {
                                                if null_first {
                                                    std::cmp::Ordering::Less
                                                } else {
                                                    std::cmp::Ordering::Greater
                                                }
                                            }
                                            (_, None) => {
                                                if null_first {
                                                    std::cmp::Ordering::Greater
                                                } else {
                                                    std::cmp::Ordering::Less
                                                }
                                            }
                                            (Some(l), Some(r)) => {
                                                if ascending {
                                                    l.cmp(r)
                                                } else {
                                                    r.cmp(l)
                                                }
                                            }
                                        });
                                        a
                                    })
                                })
                                .collect::<Vec<_>>();
                            let ty =
                                DataType::List(Arc::new(Field::new_list_field(kind.clone(), true)));
                            let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(samples)
                                .slice(1, rows);
                            let expected =
                                ListArray::from_iter_primitive::<Int64Type, _, _>(expected)
                                    .slice(1, rows);
                            let lists = arrow::compute::cast(&lists, &ty).unwrap();
                            let expected: ArrayRef = arrow::compute::cast(&expected, &ty).unwrap();
                            let batch = RecordBatch::try_from_iter([("a", lists)]).unwrap();
                            let mut calc = CalcProgram::new(
                                vec![6, 0, 3, 3],
                                vec![189, 0, 0, 1],
                                vec![3, 0, 0, 0],
                                vec![],
                                vec![],
                                vec![
                                    Some(if ascending { "ASC" } else { "DESC" }.into()),
                                    Some(
                                        if null_first {
                                            "NULLS FIRST"
                                        } else {
                                            "NULLS LAST"
                                        }
                                        .into(),
                                    ),
                                ],
                            );
                            let label=format!("{kind}/{rows}/width={width}/nulls={nulls}/asc={ascending}/null_first={null_first}");
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
    }
    group.finish();
}
criterion_group!(benches, run);
criterion_main!(benches);
