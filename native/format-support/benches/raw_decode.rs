use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_format_support::RawDecoder;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn raw(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/raw/decode");
    for rows in [16, 1024, 16384] {
        for (name, datatype, width) in [
            ("boolean", DataType::Boolean, 1),
            ("tinyint", DataType::Int8, 1),
            ("smallint", DataType::Int16, 2),
            ("integer", DataType::Int32, 4),
            ("bigint", DataType::Int64, 8),
            ("float", DataType::Float32, 4),
            ("double", DataType::Float64, 8),
            ("bytes", DataType::Binary, 264),
            ("string", DataType::Utf8, 264),
        ] {
            for nulls in [false, true] {
                let payload = vec![b'0'; width];
                let body: ArrayRef = Arc::new(BinaryArray::from_iter(
                    (0..rows + 1).map(|i| (!nulls || i % 7 != 0).then_some(payload.as_slice())),
                ));
                let batch = RecordBatch::try_from_iter(vec![("body", body)])
                    .unwrap()
                    .slice(1, rows);
                for little in [false, true] {
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("v", datatype.clone(), true)]));
                    let decoder = RawDecoder::new(schema, little);
                    let label = format!("{name}/{rows}/nulls={nulls}/little={little}");
                    decoder.decode(&batch);
                    let (output, allocations) = measure(|| decoder.decode(&batch));
                    assert_eq!(output.num_rows(), rows);
                    assert_eq!(output.column(0).null_count(), batch.column(0).null_count());
                    report(
                        &format!("format/raw/decode/{label}"),
                        batch.columns(),
                        output.columns(),
                        allocations,
                    );
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(decoder.decode(std::hint::black_box(&batch)))
                        })
                    });
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, raw);
criterion_main!(benches);
