use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn bench(c: &mut Criterion) {
    header();
    for (rows, width, cardinality) in [256, 4096].into_iter().flat_map(|rows| {
        [8, 256].into_iter().flat_map(move |width| {
            [1, 128]
                .into_iter()
                .map(move |cardinality| (rows, width, cardinality))
        })
    }) {
        let label = format!("{rows}/{width}/{cardinality}");
        let suffix = "x".repeat(width);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("key", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    (0..rows)
                        .map(|i| if i % 7 == 0 { None } else { Some(i as i64) })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("key{}{}", i % cardinality, suffix)),
                )),
            ],
        )
        .unwrap();
        let (groups, allocations) =
            measure(|| streamfusion_fluss::write::split_by_bucket(&batch, &[1], &[0], 16).unwrap());
        let columns: Vec<_> = groups
            .iter()
            .flat_map(|(_, g)| g.columns().iter().cloned())
            .collect();
        report(
            &format!("fluss_bucket_split/{label}"),
            batch.columns(),
            &columns,
            allocations,
        );
        let (_, allocations) =
            measure(|| streamfusion_fluss::write::statistics(&batch, &[0, 1]).unwrap());
        report(
            &format!("fluss_statistics/{label}"),
            batch.columns(),
            &[],
            allocations,
        );
        c.bench_function(&format!("fluss_bucket_split/{label}"), |b| {
            b.iter(|| {
                black_box(
                    streamfusion_fluss::write::split_by_bucket(&batch, &[1], &[0], 16).unwrap(),
                )
            })
        });
        c.bench_function(&format!("fluss_statistics/{label}"), |b| {
            b.iter(|| black_box(streamfusion_fluss::write::statistics(&batch, &[0, 1]).unwrap()))
        });
    }
}
criterion_group!(benches, bench);
criterion_main!(benches);
