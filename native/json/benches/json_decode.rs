use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_json::bench::JsonDecode;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn json(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/json/decode");
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            for projected in [false, true] {
                let document =
                    format!(r#"{{"id":42,"name":"{}","score":1.25}}"#, "x".repeat(width));
                let bodies = RecordBatch::try_from_iter(vec![(
                    "body",
                    Arc::new(BinaryArray::from_iter(std::iter::repeat_n(
                        Some(document.as_bytes()),
                        rows + 1,
                    ))) as ArrayRef,
                )])
                .unwrap()
                .slice(1, rows);
                let mut fields = vec![Field::new("id", DataType::Int64, true)];
                if !projected {
                    fields.extend([
                        Field::new("name", DataType::Utf8, true),
                        Field::new("score", DataType::Float64, true),
                    ]);
                }
                let decoder = JsonDecode::new(Arc::new(Schema::new(fields)));
                let label = format!("{rows}/bytes={width}/projected={projected}");
                decoder.decode(&bodies);
                let (output, allocations) = measure(|| decoder.decode(&bodies));
                assert_eq!(output.num_rows(), rows);
                report(&label, bodies.columns(), output.columns(), allocations);
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(decoder.decode(std::hint::black_box(&bodies))))
                });
            }
        }
    }
    group.finish();
}
criterion_group!(benches, json);
criterion_main!(benches);
