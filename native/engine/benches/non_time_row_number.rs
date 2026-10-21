use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, Time32MillisecondArray,
};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
use streamfusion::bench::AppendTopN;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum SortValue {
    Number(i128),
    Text(String),
}

fn sort_value(ty: &str, value: i64) -> SortValue {
    match ty {
        "tinyint" => SortValue::Number(value as i8 as i128),
        "smallint" => SortValue::Number(value as i16 as i128),
        "boolean" => SortValue::Number(i128::from(value % 2 == 0)),
        "binary8" | "string8" | "string256" => {
            let width = if ty == "string256" { 256 } else { 8 };
            SortValue::Text(format!("{value:0width$}"))
        }
        "timestamp9" => SortValue::Number(i128::from(value) * 1001 - 999999),
        _ => SortValue::Number(i128::from(value)),
    }
}

fn column(ty: &str, values: &[Option<i64>]) -> ArrayRef {
    match ty {
        "timestamp9" => Arc::new(timestamp_array(values.iter().map(|v| {
            v.map(|v| TimestampValue::from_nanos(i128::from(v) * 1001 - 999999).unwrap())
        }))),
        "tinyint" => Arc::new(Int8Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i8)),
        )),
        "smallint" => Arc::new(Int16Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i16)),
        )),
        "int" | "months" => Arc::new(Int32Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i32)),
        )),
        "date" => Arc::new(Date32Array::from_iter(
            values.iter().map(|v| v.map(|v| v as i32)),
        )),
        "time" => Arc::new(Time32MillisecondArray::from_iter(
            values.iter().map(|v| v.map(|v| v as i32)),
        )),
        "boolean" => Arc::new(BooleanArray::from_iter(
            values.iter().map(|v| v.map(|v| v % 2 == 0)),
        )),
        "decimal" => Arc::new(
            Decimal128Array::from_iter(
                values
                    .iter()
                    .map(|v| v.map(|v| i128::from(v) * 100_000_000_000)),
            )
            .with_precision_and_scale(20, 2)
            .unwrap(),
        ),
        "binary8" => Arc::new(BinaryArray::from_iter(
            values
                .iter()
                .map(|v| v.map(|v| format!("{v:08}").into_bytes())),
        )),
        "string8" | "string256" => Arc::new(StringArray::from_iter(values.iter().map(|v| {
            v.map(|v| {
                let SortValue::Text(text) = sort_value(ty, v) else {
                    unreachable!()
                };
                text
            })
        }))),
        _ => Arc::new(Int64Array::from(values.to_vec())),
    }
}

fn bench_non_time_row_number(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("non_time_row_number");
    for rows in [16usize, 1024] {
        for keys in [1usize, 64] {
            for descending_input in [false, true] {
                for ty in [
                    "bigint",
                    "int",
                    "tinyint",
                    "smallint",
                    "decimal",
                    "timestamp9",
                    "date",
                    "time",
                    "boolean",
                    "binary8",
                    "months",
                    "millis",
                    "string8",
                    "string256",
                ] {
                    let values: Vec<_> = (0..rows)
                        .map(|i| {
                            (i % 17 != 0).then_some(if descending_input {
                                (rows - i) as i64
                            } else {
                                i as i64
                            })
                        })
                        .collect();
                    let oracle: Vec<_> = values
                        .iter()
                        .map(|v| v.map(|v| sort_value(ty, v)))
                        .collect();
                    let input = RecordBatch::try_from_iter(vec![
                        (
                            "k",
                            Arc::new(Int64Array::from_iter(
                                (0..rows).map(|i| (i % 13 != 0).then_some((i % keys) as i64)),
                            )) as ArrayRef,
                        ),
                        ("v", column(ty, &values)),
                    ])
                    .unwrap();
                    let partition = input
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let key = |row| (!partition.is_null(row)).then(|| partition.value(row));
                    let mut expected = rows;
                    for row in 0..rows {
                        for previous in 0..row {
                            let earlier = match (&oracle[row], &oracle[previous]) {
                                (Some(a), Some(b)) => a < b,
                                (Some(_), None) => true,
                                _ => false,
                            };
                            if key(row) == key(previous) && earlier {
                                expected += 2;
                            }
                        }
                    }
                    let mut check = AppendTopN::non_time_row_number(vec![0], 1);
                    let (output, allocations) = measure(|| check.push(&input));
                    assert_eq!(output.num_rows(), expected);
                    report(&format!("non_time_row_number/{ty}/rows={rows}/keys={keys}/reverse={descending_input}"), input.columns(), output.columns(), allocations);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(
                        format!("{ty}/rows={rows}/keys={keys}/reverse={descending_input}"),
                        |b| {
                            b.iter_batched(
                                || AppendTopN::non_time_row_number(vec![0], 1),
                                |mut ranker| black_box(ranker.push(black_box(&input))),
                                BatchSize::SmallInput,
                            );
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

#[path = "non_time_row_number/floating.rs"]
mod floating;

criterion_group!(benches, bench_non_time_row_number, floating::bench);
criterion_main!(benches);
