use arrow::{
    array::{ArrayRef, Decimal128Array, StringArray},
    datatypes::{DataType, TimeUnit},
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use orc_rust::schema::DataType as OrcType;
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_orc::bench::normalize;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn normalization(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/orc/normalize");
    for rows in [16, 1024, 16384] {
        group.throughput(Throughput::Elements(rows as u64));
        for width in [8, 264, 4096] {
            let text = format!("{}   ", "x".repeat(width));
            let input: ArrayRef = Arc::new(StringArray::from_iter(
                (0..rows + 1).map(|i| (i % 7 != 0).then_some(text.as_str())),
            ));
            let input = input.slice(1, rows);
            for physical in [
                OrcType::Char {
                    column_index: 0,
                    max_length: (width + 3) as u32,
                },
                OrcType::String { column_index: 0 },
            ] {
                let label = format!("{physical}/{rows}/bytes={width}");
                let (output, allocations) =
                    measure(|| normalize(&input, &physical, &DataType::Utf8));
                assert_eq!(output.len(), rows);
                report(&label, &[input.clone()], &[output], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(normalize(
                            std::hint::black_box(&input),
                            &physical,
                            &DataType::Utf8,
                        ))
                    })
                });
            }
        }
        let input: ArrayRef = Arc::new(
            Decimal128Array::from_iter(
                (0..rows).map(|i| (i % 7 != 0).then_some((i as i128 - 1000) * 1_000_000_001)),
            )
            .with_precision_and_scale(38, 0)
            .unwrap(),
        );
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let physical = OrcType::Timestamp { column_index: 0 };
            let target = DataType::Timestamp(unit, None);
            let label = format!("timestamp/{rows}/{unit:?}");
            let (output, allocations) = measure(|| normalize(&input, &physical, &target));
            assert_eq!(output.len(), rows);
            report(&label, &[input.clone()], &[output], allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| {
                    std::hint::black_box(normalize(
                        std::hint::black_box(&input),
                        &physical,
                        &target,
                    ))
                })
            });
        }
    }
    group.finish();
}
criterion_group!(benches, normalization);
criterion_main!(benches);
