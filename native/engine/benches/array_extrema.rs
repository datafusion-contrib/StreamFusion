use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn fixture(
    kind: &DataType,
    rows: usize,
    width: usize,
    nulls: bool,
    minimum: bool,
) -> (RecordBatch, ArrayRef) {
    let lists = (0..=rows).map(|row| {
        (!nulls || row % 7 != 0).then(|| {
            (0..width)
                .map(|i| (!nulls || i % 9 != 0).then_some(((row + i) % 17) as i64 - 8))
                .collect::<Vec<_>>()
        })
    });
    let input = ListArray::from_iter_primitive::<Int64Type, _, _>(lists).slice(1, rows);
    let values = arrow::compute::cast(
        &input,
        &DataType::List(Arc::new(Field::new("item", kind.clone(), true))),
    )
    .unwrap();
    let expected = Int64Array::from_iter((1..=rows).map(|row| {
        if nulls && row % 7 == 0 {
            return None;
        }
        let values = (0..width)
            .filter(|i| !nulls || i % 9 != 0)
            .map(|i| ((row + i) % 17) as i64 - 8);
        if minimum {
            values.min()
        } else {
            values.max()
        }
    }));
    let expected = arrow::compute::cast(&expected, kind).unwrap();
    (
        RecordBatch::try_from_iter([("array", values)]).unwrap(),
        expected,
    )
}

fn run(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("array_extrema");
    for rows in [0, 16, 1024] {
        for width in [0, 8, 31, 32, 33, 64] {
            for nulls in [false, true] {
                for kind in [
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Decimal128(38, 9),
                ] {
                    for minimum in [true, false] {
                        let (batch, expected) = fixture(&kind, rows, width, nulls, minimum);
                        let mut calc = CalcProgram::new(
                            vec![6, 0],
                            vec![if minimum { 180 } else { 181 }, 0],
                            vec![1, 0],
                            vec![],
                            vec![],
                            vec![],
                        );
                        let label = format!(
                            "{}/{kind}/{rows}/width={width}/nulls={nulls}",
                            if minimum { "min" } else { "max" }
                        );
                        let warm = calc.run(batch.clone());
                        assert_eq!(warm.column(0).as_ref(), expected.as_ref(), "{label}");
                        let (output, allocations) = measure(|| calc.run(batch.clone()));
                        assert_eq!(output.column(0).as_ref(), expected.as_ref(), "{label}");
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
