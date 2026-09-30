use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_csv::bench::CsvDecode;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn csv(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/csv/decode");
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            for skip_errors in [false, true] {
                let text = format!("42,\"{}\",1.25", "a,".repeat(width / 2));
                let body: ArrayRef = Arc::new(BinaryArray::from_iter((0..rows + 1).map(|row| {
                    Some(if skip_errors && row % 7 == 0 {
                        b"invalid,x,1".as_slice()
                    } else {
                        text.as_bytes()
                    })
                })));
                let batch = RecordBatch::try_from_iter(vec![("body", body)])
                    .unwrap()
                    .slice(1, rows);
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("name", DataType::Utf8, true),
                    Field::new("score", DataType::Float64, true),
                ]));
                let decoder = CsvDecode::new(schema, skip_errors);
                decoder.decode(&batch);
                let (output, allocations) = measure(|| decoder.decode(&batch));
                assert_eq!(output.num_rows(), rows);
                let label = format!("{rows}/bytes={width}/skip_errors={skip_errors}");
                report(
                    &format!("format/csv/decode/{label}"),
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
criterion_group!(benches, csv);
criterion_main!(benches);
