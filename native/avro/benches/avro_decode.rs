use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_avro::bench::AvroDecode;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
const SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"score","type":"double"}]}"#;

fn message(width: usize, confluent: bool) -> Vec<u8> {
    let mut bytes = if confluent {
        vec![0, 0, 0, 0, 1]
    } else {
        vec![]
    };
    bytes.push(84); // Avro zigzag long 42.
    let mut length = (width as u64) << 1;
    while length >= 128 {
        bytes.push((length as u8 & 127) | 128);
        length >>= 7;
    }
    bytes.push(length as u8);
    bytes.extend(std::iter::repeat_n(b'x', width));
    bytes.extend_from_slice(&1.25f64.to_le_bytes());
    bytes
}
fn avro(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/avro/decode");
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            for confluent in [false, true] {
                let payload = message(width, confluent);
                let body: ArrayRef = Arc::new(BinaryArray::from_iter(
                    (0..rows + 1).map(|_| Some(payload.as_slice())),
                ));
                let batch = RecordBatch::try_from_iter(vec![("body", body)])
                    .unwrap()
                    .slice(1, rows);
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("score", DataType::Float64, false),
                ]));
                let decoder = AvroDecode::new(SCHEMA, schema, confluent);
                decoder.decode(&batch);
                let (output, allocations) = measure(|| decoder.decode(&batch));
                assert_eq!(output.num_rows(), rows);
                let label = format!("{rows}/bytes={width}/confluent={confluent}");
                report(
                    &format!("format/avro/decode/{label}"),
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
    }
    group.finish();
}
criterion_group!(benches, avro);
criterion_main!(benches);
