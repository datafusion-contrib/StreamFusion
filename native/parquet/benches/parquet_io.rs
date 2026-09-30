use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_parquet::bench::{decode, encode};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn parquet(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/parquet");
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            let text = "x".repeat(width);
            let input = RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                (
                    "name",
                    Arc::new(StringArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
                    )) as ArrayRef,
                ),
            ])
            .unwrap();
            group.throughput(Throughput::Elements(rows as u64));
            let selected: Vec<_> = (0..rows).step_by(2).collect();
            for selection in [None, Some(selected.as_slice())] {
                let label = format!(
                    "encode/{rows}/bytes={width}/selected={}",
                    selection.is_some()
                );
                let (encoded, allocations) = measure(|| encode(&input, selection));
                report(&label, input.columns(), &[], allocations);
                let decoded = decode(&encoded, rows);
                assert_eq!(
                    decoded.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    selection.map_or(rows, <[usize]>::len)
                );
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(encode(std::hint::black_box(&input), selection)))
                });
            }
            let encoded = encode(&input, None);
            let label = format!("decode/{rows}/bytes={width}");
            let (output, allocations) = measure(|| decode(&encoded, rows));
            assert_eq!(output, vec![input.clone()]);
            report(&label, &[], output[0].columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(decode(std::hint::black_box(&encoded), rows)))
            });
        }
    }
    group.finish();
}
criterion_group!(benches, parquet);
criterion_main!(benches);
