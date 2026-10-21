use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int32Type};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn fixture(
    kind: &str,
    rows: usize,
    width: usize,
    nullable: bool,
) -> (RecordBatch, Vec<Option<i32>>) {
    let lists = (0..=rows).map(|row| {
        (!nullable || row % 7 != 0).then(|| {
            (0..width)
                .map(|i| (!nullable || i % 9 != 0).then_some(((row + i) % 4) as i32))
                .collect::<Vec<_>>()
        })
    });
    let integer: ArrayRef =
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(lists).slice(1, rows));
    let needles: Vec<_> = (0..rows)
        .map(|row| (!nullable || row % 5 != 0).then_some((row % 8) as i32))
        .collect();
    let (values, needle): (ArrayRef, ArrayRef) = if kind == "boolean" {
        let lists = (1..=rows).map(|row| {
            (!nullable || row % 7 != 0).then(|| {
                (0..width)
                    .map(|i| (!nullable || i % 9 != 0).then_some((row + i) % 2 != 0))
                    .collect::<Vec<_>>()
            })
        });
        let mut builder = arrow::array::ListBuilder::new(arrow::array::BooleanBuilder::new());
        for list in lists {
            if let Some(list) = list {
                builder.values().extend(list);
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        (
            Arc::new(builder.finish()),
            Arc::new(BooleanArray::from_iter(
                needles.iter().map(|n| n.map(|v| v % 2 != 0)),
            )),
        )
    } else {
        let data_type = match kind {
            "bigint" => DataType::Int64,
            "decimal" => DataType::Decimal128(38, 9),
            _ => DataType::Int32,
        };
        (
            arrow::compute::cast(
                &integer,
                &DataType::List(Arc::new(Field::new_list_field(data_type.clone(), true))),
            )
            .unwrap(),
            arrow::compute::cast(&Int32Array::from(needles.clone()), &data_type).unwrap(),
        )
    };
    let expected = (0..rows)
        .map(|row| {
            if nullable && (row + 1) % 7 == 0 {
                return None;
            }
            let Some(needle) = needles[row] else {
                return None;
            };
            Some(
                (0..width)
                    .find(|&i| {
                        if nullable && i % 9 == 0 {
                            return false;
                        }
                        if kind == "boolean" {
                            (row + 1 + i) % 2 == (needle as usize) % 2
                        } else {
                            ((row + 1 + i) % 4) as i32 == needle
                        }
                    })
                    .map_or(0, |i| i as i32 + 1),
            )
        })
        .collect();
    (
        RecordBatch::try_from_iter([("array", values), ("needle", needle)]).unwrap(),
        expected,
    )
}

fn run(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("array_search");
    for rows in [0, 16, 1024] {
        for width in [0, 8, 64] {
            for nullable in [false, true] {
                for kind in ["integer", "bigint", "boolean", "decimal"] {
                    let (batch, expected_positions) = fixture(kind, rows, width, nullable);
                    for contains in [false, true] {
                        let mut calc = CalcProgram::new(
                            vec![6, 0, 0],
                            vec![if contains { 178 } else { 179 }, 0, 1],
                            vec![2, 0, 0],
                            vec![],
                            vec![],
                            vec![],
                        );
                        let expected: ArrayRef = if contains {
                            Arc::new(BooleanArray::from_iter(
                                expected_positions.iter().enumerate().map(|(row, pos)| {
                                    if nullable && (row + 1) % 7 == 0 {
                                        return None;
                                    }
                                    if nullable && row % 5 == 0 {
                                        return Some(width > 0);
                                    }
                                    pos.map(|position| position > 0)
                                }),
                            ))
                        } else {
                            Arc::new(Int32Array::from(expected_positions.clone()))
                        };
                        let label = format!(
                            "{}/{kind}/{rows}/width={width}/nulls={nullable}",
                            if contains { "contains" } else { "position" }
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
