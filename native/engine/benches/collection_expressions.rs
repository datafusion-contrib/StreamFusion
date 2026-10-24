use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Int32Array, Int32Builder, Int8Builder, ListArray, ListBuilder,
    MapBuilder, RecordBatch, StringBuilder, StructBuilder, UInt16Builder, UInt32Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn plan() -> CalcProgram {
    plan_for(165)
}

fn plan_for(operation: i64) -> CalcProgram {
    CalcProgram::new(
        vec![6, 0],
        vec![operation, 0],
        vec![1, 0],
        vec![],
        vec![],
        vec![],
    )
}

fn fixture(
    kind: &str,
    rows: usize,
    width: usize,
    domain: usize,
    bytes: usize,
    nullable: bool,
) -> (ArrayRef, ArrayRef) {
    let mut integers = ListBuilder::new(Int32Builder::new());
    let mut expected_integers = ListBuilder::new(Int32Builder::new());
    let mut booleans = ListBuilder::new(BooleanBuilder::new());
    let mut expected_booleans = ListBuilder::new(BooleanBuilder::new());
    let mut strings = ListBuilder::new(StringBuilder::new());
    let mut expected_strings = ListBuilder::new(StringBuilder::new());
    let text: Vec<_> = (0..domain)
        .map(|i| format!("{i:04}-é中😀{}", "x".repeat(bytes)))
        .collect();
    for row in 0..rows + 1 {
        let valid = !nullable || row % 7 != 0;
        if valid {
            let mut seen = std::collections::BTreeSet::new();
            for col in 0..width {
                let value = if nullable && col % 9 == 0 {
                    None
                } else {
                    Some((row + col) % domain)
                };
                let first = seen.insert(value);
                match kind {
                    "integer" => {
                        integers.values().append_option(value.map(|v| v as i32));
                        if first {
                            expected_integers
                                .values()
                                .append_option(value.map(|v| v as i32));
                        }
                    }
                    "boolean" => {
                        booleans.values().append_option(value.map(|v| v != 0));
                        if first {
                            expected_booleans
                                .values()
                                .append_option(value.map(|v| v != 0));
                        }
                    }
                    _ => {
                        strings
                            .values()
                            .append_option(value.map(|v| text[v].as_str()));
                        if first {
                            expected_strings
                                .values()
                                .append_option(value.map(|v| text[v].as_str()));
                        }
                    }
                }
            }
        }
        match kind {
            "integer" => {
                integers.append(valid);
                expected_integers.append(valid);
            }
            "boolean" => {
                booleans.append(valid);
                expected_booleans.append(valid);
            }
            _ => {
                strings.append(valid);
                expected_strings.append(valid);
            }
        }
    }
    match kind {
        "integer" => (
            Arc::new(integers.finish().slice(1, rows)),
            Arc::new(expected_integers.finish().slice(1, rows)),
        ),
        "boolean" => (
            Arc::new(booleans.finish().slice(1, rows)),
            Arc::new(expected_booleans.finish().slice(1, rows)),
        ),
        _ => (
            Arc::new(strings.finish().slice(1, rows)),
            Arc::new(expected_strings.finish().slice(1, rows)),
        ),
    }
}

fn represented_rows(rows: usize, width: usize) -> ArrayRef {
    let cache = ListBuilder::new(UInt16Builder::new()).with_field(Arc::new(Field::new(
        "unit",
        DataType::UInt16,
        false,
    )));
    let text_fields = vec![
        Field::new("utf8", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            ("streamfusion.nested-string".to_owned(), "1".to_owned()),
        ])),
        Field::new("java", cache.finish_cloned().data_type().clone(), true),
    ];
    let text = StructBuilder::new(
        text_fields,
        vec![Box::new(StringBuilder::new()), Box::new(cache)],
    );
    let fields = vec![
        Field::new("text", text.finish_cloned().data_type().clone(), true),
        Field::new("__sf_nested_row_kind", DataType::Int8, true).with_metadata(
            std::collections::HashMap::from([(
                "streamfusion.nested-row-kind".to_owned(),
                "1".to_owned(),
            )]),
        ),
    ];
    let values = StructBuilder::new(fields, vec![Box::new(text), Box::new(Int8Builder::new())]);
    let mut arrays = ListBuilder::new(values);
    for row in 0..=rows {
        for column in 0..width {
            let text = arrays.values().field_builder::<StructBuilder>(0).unwrap();
            let value = ["?", "é", "😀", ""][column % 4];
            text.field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(value);
            let cache = text.field_builder::<ListBuilder<UInt16Builder>>(1).unwrap();
            if column % 3 == 0 {
                cache.append(false);
            } else {
                if column % 4 == 0 {
                    cache.values().append_value(0xd800);
                } else {
                    cache
                        .values()
                        .append_slice(&value.encode_utf16().collect::<Vec<_>>());
                }
                cache.append(true);
            }
            text.append(true);
            arrays
                .values()
                .field_builder::<Int8Builder>(1)
                .unwrap()
                .append_value(((row + column) % 4) as i8);
            arrays.values().append(column % 5 != 0);
        }
        arrays.append(row % 7 != 0);
    }
    Arc::new(arrays.finish().slice(1, rows))
}

fn rearrange_expected(input: &ArrayRef, reverse: bool) -> ArrayRef {
    let input = input.as_any().downcast_ref::<ListArray>().unwrap();
    let mut indices = Vec::new();
    let mut offsets = vec![0];
    for row in 0..input.len() {
        if input.is_valid(row) {
            let first = input.value_offsets()[row] as u32;
            let end = input.value_offsets()[row + 1] as u32;
            if reverse {
                indices.extend((first..end).rev());
            } else {
                indices.extend(end.saturating_sub(2).max(first)..end);
            }
        }
        offsets.push(indices.len() as i32);
    }
    let values = arrow::compute::take(input.values(), &UInt32Array::from(indices), None).unwrap();
    let DataType::List(field) = input.data_type() else {
        unreachable!()
    };
    Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        values,
        input.nulls().cloned(),
    ))
}

fn run(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("collection_expressions");
    for rows in [16, 1024, 16384] {
        for width in [8, 64] {
            let input = represented_rows(rows, width);
            let batch = RecordBatch::try_from_iter([("value", input.clone())]).unwrap();
            for reverse in [true, false] {
                let mut calc = if reverse {
                    plan_for(169)
                } else {
                    CalcProgram::new(
                        vec![6, 0, 7, 7],
                        vec![170, 0, 0, 1],
                        vec![3, 0, 0, 0],
                        vec![-2, -1],
                        vec![],
                        vec![],
                    )
                };
                let expected = rearrange_expected(&input, reverse);
                let (output, allocations) = measure(|| calc.run(batch.clone()));
                assert_eq!(output.column(0).as_ref(), expected.as_ref());
                let label = format!(
                    "represented-{}/{rows}/width={width}",
                    if reverse { "reverse" } else { "slice" }
                );
                report(&label, batch.columns(), output.columns(), allocations);
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(calc.run(batch.clone())))
                });
            }
        }
        for width in [8, 64] {
            for nullable in [false, true] {
                for kind in ["integer", "boolean", "string"] {
                    let domains = if kind == "boolean" {
                        vec![2]
                    } else {
                        vec![4, width]
                    };
                    let payloads = if kind == "string" {
                        vec![8, 264]
                    } else {
                        vec![0]
                    };
                    for domain in domains {
                        for &bytes in &payloads {
                            let (input, expected) =
                                fixture(kind, rows, width, domain, bytes, nullable);
                            let batch = RecordBatch::try_from_iter([("value", input)]).unwrap();
                            let mut cardinality = plan_for(167);
                            let cardinalities = cardinality.run(batch.clone());
                            let expected_cardinalities =
                                Int32Array::from_iter((0..rows).map(|row| {
                                    (!nullable || (row + 1) % 7 != 0).then_some(width as i32)
                                }));
                            assert_eq!(cardinalities.column(0).as_ref(), &expected_cardinalities);
                            group.throughput(Throughput::Elements(rows as u64));
                            group.bench_function(
                                BenchmarkId::from_parameter(format!(
                                    "cardinality/{kind}/{rows}/width={width}/domain={domain}/bytes={bytes}/nulls={nullable}"
                                )),
                                |b| b.iter(|| std::hint::black_box(cardinality.run(batch.clone()))),
                            );
                            let mut calc = plan();
                            let warm = calc.run(batch.clone());
                            assert_eq!(warm.column(0).as_ref(), expected.as_ref());
                            let label = format!("{kind}/{rows}/width={width}/domain={domain}/bytes={bytes}/nulls={nullable}");
                            let (out, allocations) = measure(|| calc.run(batch.clone()));
                            report(&label, batch.columns(), out.columns(), allocations);
                            group.throughput(Throughput::Elements(rows as u64));
                            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                                b.iter(|| {
                                    std::hint::black_box(
                                        calc.run(std::hint::black_box(batch.clone())),
                                    )
                                })
                            });
                        }
                    }
                }
            }
        }
        for width in [0, 8, 64] {
            let mut maps = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
            for row in 0..=rows {
                for key in 0..width {
                    maps.keys().append_value(key);
                    maps.values().append_null();
                }
                maps.append(row % 7 != 0).unwrap();
            }
            let input: ArrayRef = Arc::new(maps.finish().slice(1, rows));
            let batch = RecordBatch::try_from_iter([("value", input)]).unwrap();
            let mut calc = plan_for(167);
            let out = calc.run(batch.clone());
            let expected =
                Int32Array::from_iter((0..rows).map(|row| ((row + 1) % 7 != 0).then_some(width)));
            assert_eq!(out.column(0).as_ref(), &expected);
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_function(
                BenchmarkId::from_parameter(format!("cardinality/map/{rows}/width={width}")),
                |b| b.iter(|| std::hint::black_box(calc.run(batch.clone()))),
            );
        }
    }
    group.finish();
}
fn constructors(c: &mut Criterion) {
    let mut group = c.benchmark_group("collection_expressions/array_constructor");
    for rows in [16, 1024, 16384] {
        for kind in [
            "integer",
            "boolean",
            "float",
            "decimal",
            "date",
            "time",
            "timestamp",
        ] {
            let column: ArrayRef = match kind {
                "boolean" => Arc::new(arrow::array::BooleanArray::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i % 3 == 0)),
                )),
                "float" => Arc::new(arrow::array::Float64Array::from_iter((0..rows).map(|i| {
                    (i % 7 != 0).then_some(if i % 3 == 0 {
                        -0.0
                    } else {
                        f64::from_bits(0x7ff8000000000001)
                    })
                }))),
                "decimal" => Arc::new(
                    arrow::array::Decimal128Array::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(i as i128)),
                    )
                    .with_precision_and_scale(20, 3)
                    .unwrap(),
                ),
                "date" => Arc::new(arrow::array::Date32Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as i32 - 100)),
                )),
                "time" => Arc::new(arrow::array::Time32MillisecondArray::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as i32 % 86400000)),
                )),
                "timestamp" => Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                    (0..rows).map(|i| {
                        (i % 7 != 0).then(|| {
                            streamfusion_bridge::timestamp::TimestampValue::from_nanos(
                                i as i128 * 1234567 - 999999,
                            )
                            .unwrap()
                        })
                    }),
                )),
                _ => Arc::new(arrow::array::Int64Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as i64)),
                )),
            };
            for width in [2, 16] {
                let input = RecordBatch::try_from_iter(
                    (0..width).map(|i| (format!("value{i}"), column.clone())),
                )
                .unwrap();
                for compact in [false, true] {
                    if compact && kind != "timestamp" {
                        continue;
                    }
                    let mut plan = CalcProgram::new(
                        std::iter::once(6)
                            .chain(std::iter::repeat_n(0, width))
                            .collect(),
                        std::iter::once(if compact { 172 } else { 171 })
                            .chain((0..width).map(|i| i as i64))
                            .collect(),
                        std::iter::once(width as i64)
                            .chain(std::iter::repeat_n(0, width))
                            .collect(),
                        vec![],
                        vec![],
                        vec![],
                    );
                    let output = plan.run(input.clone());
                    assert_eq!(output.num_rows(), rows);
                    let list = output
                        .column(0)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    assert_eq!(list.null_count(), 0);
                    assert_eq!(list.values().len(), rows * width);
                    let label = format!("{kind}/{rows}/width={width}/compact={compact}");
                    let (output, allocations) = measure(|| plan.run(input.clone()));
                    report(&label, input.columns(), output.columns(), allocations);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(plan.run(std::hint::black_box(input.clone())))
                        })
                    });
                }
            }
        }
    }
    group.finish();
}

fn temporal_operands(c: &mut Criterion) {
    let mut group = c.benchmark_group("collection_expressions/unnest_temporal");
    for rows in [16, 1024, 16384] {
        for operation in [173, 174, 175, 176, 177] {
            for nullable in [false, true] {
                let values = (0..rows)
                    .map(|i| (!nullable || i % 7 != 0).then_some(i as i64 * 12345 - 99999))
                    .collect::<Vec<_>>();
                let input: ArrayRef = if operation == 173 {
                    Arc::new(arrow::array::Int64Array::from(values))
                } else {
                    Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                        values.into_iter().map(|value| {
                            value.map(streamfusion_bridge::timestamp::TimestampValue::from_millis)
                        }),
                    ))
                };
                let mut columns = vec![("value", input)];
                if operation >= 176 {
                    columns.push((
                        "offset",
                        Arc::new(Int32Array::from_iter((0..rows).map(|i| {
                            (!nullable || i % 7 != 0).then_some(if i % 2 == 0 {
                                -18000
                            } else {
                                3600
                            })
                        }))) as ArrayRef,
                    ));
                }
                let batch = RecordBatch::try_from_iter(columns).unwrap();
                let mut plan = if operation == 177 {
                    CalcProgram::new(
                        vec![6, 0, 0, 7],
                        vec![operation, 0, 1, 0],
                        vec![3, 0, 0, 0],
                        vec![1000],
                        vec![],
                        vec![],
                    )
                } else if operation == 176 {
                    CalcProgram::new(
                        vec![6, 0, 0],
                        vec![operation, 0, 1],
                        vec![2, 0, 0],
                        vec![],
                        vec![],
                        vec![],
                    )
                } else if operation == 175 {
                    CalcProgram::new(
                        vec![6, 0, 7],
                        vec![operation, 0, 0],
                        vec![2, 0, 0],
                        vec![1000],
                        vec![],
                        vec![],
                    )
                } else {
                    plan_for(operation)
                };
                let output = plan.run(batch.clone());
                assert_eq!(output.num_rows(), rows);
                assert_eq!(
                    output.column(0).null_count(),
                    if nullable { rows.div_ceil(7) } else { 0 }
                );
                let label = format!("{operation}/{rows}/nullable={nullable}");
                let (output, allocations) = measure(|| plan.run(batch.clone()));
                report(&label, batch.columns(), output.columns(), allocations);
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(plan.run(std::hint::black_box(batch.clone()))))
                });
            }
        }
    }
    group.finish();
}

fn binary_map_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_map_values");
    for rows in [16, 1024] {
        for bytes in [0, 4, 256] {
            let mut maps = MapBuilder::new(
                None,
                StringBuilder::new(),
                arrow::array::BinaryBuilder::new(),
            );
            let payload = vec![0x80; bytes];
            for row in 1..=rows + 1 {
                let valid = row % 7 != 0;
                if valid && row % 5 != 0 {
                    maps.keys().append_value("first");
                    maps.values().append_value(&payload);
                    maps.keys().append_value("absent");
                    maps.values().append_null();
                    maps.keys().append_value("last");
                    maps.values().append_value(&payload);
                }
                maps.append(valid).unwrap();
            }
            let input = maps.finish().slice(1, rows);
            let batch =
                RecordBatch::try_from_iter([("m", Arc::new(input.clone()) as ArrayRef)]).unwrap();
            let mut calc = plan_for(193);
            let (output, allocations) = measure(|| calc.run(batch.clone()));
            let values = output
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(values.offsets(), input.offsets());
            assert_eq!(values.nulls(), input.nulls());
            assert!(Arc::ptr_eq(values.values(), input.entries().column(1)));
            let label = format!("rows={rows}/bytes={bytes}/sliced-null-empty");
            report(&label, batch.columns(), output.columns(), allocations);
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(calc.run(batch.clone())))
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    run,
    constructors,
    temporal_operands,
    binary_map_values
);
criterion_main!(benches);
