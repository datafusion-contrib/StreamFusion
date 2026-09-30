use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prost_reflect::prost::Message;
use prost_reflect::prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_kafka::bench::FormatEncoder;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn descriptor() -> Vec<u8> {
    let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("criterion.proto".into()),
            package: Some("criterion".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Row".into()),
                field: vec![
                    field("id", 1, Type::Int64),
                    field("name", 2, Type::String),
                    field("score", 3, Type::Double),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn formats(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("kafka/format_encode");
    let avro = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"score","type":"double"}]}"#;
    let encoders = [
        ("json", FormatEncoder::json()),
        ("csv", FormatEncoder::csv()),
        ("avro", FormatEncoder::avro(avro, false)),
        ("avro_confluent", FormatEncoder::avro(avro, true)),
        ("protobuf", FormatEncoder::protobuf(&descriptor())),
    ];
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            let text = format!("{},\"", "x".repeat(width));
            let input = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("score", DataType::Float64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                        text.as_str(),
                        rows,
                    ))) as ArrayRef,
                    Arc::new(Float64Array::from(vec![1.25; rows])) as ArrayRef,
                ],
            )
            .unwrap();
            group.throughput(Throughput::Elements(rows as u64));
            for (format, encoder) in &encoders {
                let label = format!("{format}/{rows}/bytes={width}");
                let (output, allocations) = measure(|| encoder.encode(&input));
                assert_eq!(output.len(), rows);
                assert!(!output.line(0).is_empty());
                report(&label, input.columns(), &[], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(encoder.encode(std::hint::black_box(&input))))
                });
            }
            for little in [false, true] {
                let encoder = FormatEncoder::raw(little);
                for index in [0, 1, 2] {
                    let input = input.project(&[index]).unwrap();
                    let label = format!("raw/column={index}/{rows}/bytes={width}/little={little}");
                    let (output, allocations) = measure(|| encoder.encode(&input));
                    assert_eq!(output.len(), rows);
                    report(&label, input.columns(), &[], allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(encoder.encode(std::hint::black_box(&input)))
                        })
                    });
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, formats);
criterion_main!(benches);
