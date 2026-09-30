use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prost_reflect::prost::Message;
use prost_reflect::prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_protobuf::ProtobufDecoder;

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
fn message(width: usize) -> Vec<u8> {
    let mut bytes = vec![8, 42, 18];
    let mut length = width as u64;
    while length >= 128 {
        bytes.push((length as u8 & 127) | 128);
        length >>= 7;
    }
    bytes.push(length as u8);
    bytes.extend(std::iter::repeat_n(b'x', width));
    bytes.push(25);
    bytes.extend_from_slice(&1.25f64.to_le_bytes());
    bytes
}
fn protobuf(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/protobuf/decode");
    let descriptor = descriptor();
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            let payload = message(width);
            let body: ArrayRef = Arc::new(BinaryArray::from_iter(
                (0..rows + 1).map(|_| Some(payload.as_slice())),
            ));
            let batch = RecordBatch::try_from_iter(vec![("body", body)])
                .unwrap()
                .slice(1, rows);
            let decoder = ProtobufDecoder::new(&descriptor, "criterion.Row");
            decoder.decode(&batch);
            let (output, allocations) = measure(|| decoder.decode(&batch));
            assert_eq!(output.num_rows(), rows);
            let label = format!("{rows}/bytes={width}");
            report(
                &format!("format/protobuf/decode/{label}"),
                batch.columns(),
                output.columns(),
                allocations,
            );
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(decoder.decode(std::hint::black_box(&batch))))
            });
        }
    }
    group.finish();
}
criterion_group!(benches, protobuf);
criterion_main!(benches);
