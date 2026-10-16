//! Production Arrow ownership handoffs and transformations; no JVM is needed for C Data calls.
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Float64Array, Int64Array, Int64Builder, Int8Builder,
    ListBuilder, RecordBatch, StringArray, StringBuilder, StructArray, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::{
    export_record_batch, import_record_batch, import_record_batch_with_schema,
    ordering::canonical_ordering_column,
    partition::split_by_partition_columns,
    timestamp::{cast_timestamp, timestamps_from_millis},
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn roundtrip(batch: &RecordBatch, cached: bool) -> RecordBatch {
    let mut array = FFI_ArrowArray::empty();
    if cached {
        array = FFI_ArrowArray::new(&StructArray::from(batch.clone()).to_data());
        import_record_batch_with_schema(&mut array as *mut _ as i64, &batch.schema())
    } else {
        let mut schema = FFI_ArrowSchema::empty();
        export_record_batch(
            batch.clone(),
            &mut array as *mut _ as i64,
            &mut schema as *mut _ as i64,
        );
        import_record_batch(&mut array as *mut _ as i64, &mut schema as *mut _ as i64)
    }
}

fn handoffs(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("bridge/c_data");
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            let text = "x".repeat(width);
            let strings: ArrayRef = Arc::new(StringArray::from_iter(
                (0..rows + 1).map(|i| (i % 7 != 0).then_some(text.as_str())),
            ));
            let integers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64 + 1));
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("payload", DataType::Utf8, true),
                ])),
                vec![integers, strings],
            )
            .unwrap()
            .slice(1, rows);
            group.throughput(Throughput::Elements(rows as u64));
            for cached in [false, true] {
                let label = format!("{rows}/bytes={width}/cached={cached}");
                assert_eq!(roundtrip(&batch, cached), batch);
                let (output, allocations) = measure(|| roundtrip(&batch, cached));
                report(
                    &format!("bridge/c_data/{label}"),
                    batch.columns(),
                    output.columns(),
                    allocations,
                );
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(roundtrip(std::hint::black_box(&batch), cached)))
                });
            }
        }
    }
    group.finish();
}

fn transformations(c: &mut Criterion) {
    let mut group = c.benchmark_group("bridge/transform");
    for rows in [16, 1024, 16384] {
        let millis =
            Int64Array::from_iter((0..rows).map(|i| (i % 7 != 0).then_some(i as i64 - 100)));
        let components: ArrayRef = Arc::new(timestamps_from_millis(&millis));
        group.throughput(Throughput::Elements(rows as u64));
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let target = DataType::Timestamp(unit, None);
            let label = format!("timestamp/{rows}/{target:?}");
            let (output, allocations) = measure(|| cast_timestamp(&components, &target).unwrap());
            report(
                &label,
                std::slice::from_ref(&components),
                std::slice::from_ref(&output),
                allocations,
            );
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| {
                    std::hint::black_box(
                        cast_timestamp(std::hint::black_box(&components), &target).unwrap(),
                    )
                })
            });
        }
        let values: ArrayRef = Arc::new(Float64Array::from_iter((0..rows).map(|i| {
            (i % 7 != 0).then_some(match i % 5 {
                0 => f64::NAN,
                1 => -0.0,
                _ => i as f64,
            })
        })));
        let label = format!("ordering/{rows}/nan_nulls");
        let (output, allocations) = measure(|| canonical_ordering_column(&values));
        report(
            &label,
            std::slice::from_ref(&values),
            std::slice::from_ref(&output),
            allocations,
        );
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter(|| {
                std::hint::black_box(canonical_ordering_column(std::hint::black_box(&values)))
            })
        });
        for keys in [1, 64, rows] {
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|i| (i % keys) as i64),
                    )) as ArrayRef,
                ),
                ("value", components.clone()),
            ])
            .unwrap();
            let label = format!("partition/{rows}/keys={keys}");
            let (output, allocations) = measure(|| split_by_partition_columns(&batch, &[0]));
            assert_eq!(
                output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                rows
            );
            let columns: Vec<_> = output
                .iter()
                .flat_map(|batch| batch.columns().iter().cloned())
                .collect();
            report(&label, batch.columns(), &columns, allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| {
                    std::hint::black_box(split_by_partition_columns(
                        std::hint::black_box(&batch),
                        &[0],
                    ))
                })
            });
        }
    }
    group.finish();
}

fn nested_row_kinds(c: &mut Criterion) {
    let mut group = c.benchmark_group("bridge/nested_row_kinds");
    for rows in [16, 1024, 16384] {
        for width in [8, 264] {
            let fields = vec![
                Field::new("value", DataType::Int64, true),
                Field::new("text", DataType::Utf8, true),
                Field::new("__sf_nested_row_kind", DataType::Int8, true)
                    .with_metadata([("streamfusion.nested-row-kind".into(), "1".into())].into()),
            ];
            let mut arrays = ListBuilder::new(StructBuilder::new(
                fields,
                vec![
                    Box::new(Int64Builder::new()),
                    Box::new(StringBuilder::new()),
                    Box::new(Int8Builder::new()),
                ],
            ));
            let text = format!("é{}", "x".repeat(width));
            for row in 0..rows + 1 {
                for element in 0..if row % 5 == 0 { 0 } else { 2 } {
                    let value = row * 2 + element;
                    let record = arrays.values();
                    record
                        .field_builder::<Int64Builder>(0)
                        .unwrap()
                        .append_option((value % 7 != 0).then_some(value as i64));
                    record
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_option((value % 11 != 0).then_some(if value % 3 == 0 {
                            ""
                        } else {
                            text.as_str()
                        }));
                    record
                        .field_builder::<Int8Builder>(2)
                        .unwrap()
                        .append_value((value % 4) as i8);
                    record.append(value % 13 != 0);
                }
                arrays.append(row % 17 != 0);
            }
            let batch = RecordBatch::try_from_iter(vec![(
                "records",
                Arc::new(arrays.finish()) as ArrayRef,
            )])
            .unwrap()
            .slice(1, rows);
            group.throughput(Throughput::Elements(rows as u64));
            for cached in [false, true] {
                let label = format!("{rows}/bytes={width}/cached={cached}");
                assert_eq!(roundtrip(&batch, cached), batch);
                let (output, allocations) = measure(|| roundtrip(&batch, cached));
                report(
                    &format!("bridge/nested_row_kinds/{label}"),
                    batch.columns(),
                    output.columns(),
                    allocations,
                );
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(roundtrip(std::hint::black_box(&batch), cached)))
                });
            }
        }
    }
    group.finish();
}

fn nested_binary(c: &mut Criterion) {
    let mut group = c.benchmark_group("bridge/nested_binary");
    for rows in [16, 1024, 16384] {
        for width in [4, 256] {
            let fields = vec![
                Field::new("payload", DataType::Binary, true),
                Field::new("__sf_nested_row_kind", DataType::Int8, true)
                    .with_metadata([("streamfusion.nested-row-kind".into(), "1".into())].into()),
            ];
            let mut arrays = ListBuilder::new(StructBuilder::new(
                fields,
                vec![Box::new(BinaryBuilder::new()), Box::new(Int8Builder::new())],
            ));
            for row in 0..rows + 1 {
                for element in 0..if row % 5 == 0 { 0 } else { 2 } {
                    let index = row * 2 + element;
                    let length = match index % 4 {
                        0 => 0,
                        1 => 1,
                        2 => width,
                        _ => width + 3,
                    };
                    let payload: Vec<u8> = (0..length).map(|i| (128 + i * 73) as u8).collect();
                    let record = arrays.values();
                    record
                        .field_builder::<BinaryBuilder>(0)
                        .unwrap()
                        .append_option((index % 7 != 0).then_some(payload.as_slice()));
                    record
                        .field_builder::<Int8Builder>(1)
                        .unwrap()
                        .append_value((index % 4) as i8);
                    record.append(index % 13 != 0);
                }
                arrays.append(row % 17 != 0);
            }
            let batch = RecordBatch::try_from_iter(vec![(
                "records",
                Arc::new(arrays.finish()) as ArrayRef,
            )])
            .unwrap()
            .slice(1, rows);
            group.throughput(Throughput::Elements(rows as u64));
            for cached in [false, true] {
                let label =
                    format!("{rows}/declared_width=4/payload_width={width}/cached={cached}");
                assert_eq!(roundtrip(&batch, cached), batch);
                let (output, allocations) = measure(|| roundtrip(&batch, cached));
                report(
                    &format!("bridge/nested_binary/{label}"),
                    batch.columns(),
                    output.columns(),
                    allocations,
                );
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(roundtrip(std::hint::black_box(&batch), cached)))
                });
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    handoffs,
    transformations,
    nested_row_kinds,
    nested_binary
);
criterion_main!(benches);
