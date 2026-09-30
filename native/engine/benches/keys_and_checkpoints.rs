use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::{flink_key_hashes, AppendTopN, GroupBy, KeyCodec, WindowRank};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn keys_and_checkpoints(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("engine/keys_and_checkpoints");
    for rows in [16, 1024, 16384] {
        for keys in [1, 64, rows] {
            let input = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|i| (i % keys) as i64),
                    )) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                (
                    "text",
                    Arc::new(StringArray::from_iter((0..rows).map(|i| {
                        (i % 7 != 0).then(|| format!("key-{}-{}", i % keys, "x".repeat(264)))
                    }))) as ArrayRef,
                ),
            ])
            .unwrap();
            group.throughput(Throughput::Elements(rows as u64));
            for columns in [&[0][..], &[0, 2][..]] {
                let arrays: Vec<_> = columns
                    .iter()
                    .map(|&column| input.column(column).clone())
                    .collect();
                let codec = KeyCodec::new(&arrays);
                let label = format!(
                    "arrow_row_encode/{rows}/keys={keys}/columns={}",
                    columns.len()
                );
                let (encoded, allocations) = measure(|| codec.encode(&arrays, rows));
                assert_eq!(codec.decode(&encoded), arrays);
                report(&label, &arrays, &[], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(codec.encode(std::hint::black_box(&arrays), rows))
                    })
                });
                let label = format!(
                    "arrow_row_decode/{rows}/keys={keys}/columns={}",
                    columns.len()
                );
                let (output, allocations) = measure(|| codec.decode(&encoded));
                report(&label, &[], &output, allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(codec.decode(std::hint::black_box(&encoded))))
                });
                let label = format!(
                    "flink_binary_row_hash/{rows}/keys={keys}/columns={}",
                    columns.len()
                );
                let descriptors = vec![0; columns.len()];
                let (output, allocations) =
                    measure(|| flink_key_hashes(&input, columns, &descriptors));
                assert_eq!(output.len(), rows);
                report(&label, &arrays, &[], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(flink_key_hashes(
                            std::hint::black_box(&input),
                            columns,
                            &descriptors,
                        ))
                    })
                });
            }
            let mut topn = AppendTopN::new(vec![0], vec![(1, true)], 4, false, false);
            topn.push(&input);
            for groups in [1, 128] {
                let label = format!("topn_snapshot/{rows}/keys={keys}/groups={groups}");
                let (snapshots, allocations) = measure(|| topn.snapshot(groups));
                assert!(!snapshots.is_empty());
                report(&label, input.columns(), &[], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(topn.snapshot(groups)))
                });
                let label = format!("topn_restore/{rows}/keys={keys}/groups={groups}");
                let (_, allocations) = measure(|| AppendTopN::restore(&snapshots));
                report(&label, &[], &[], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(AppendTopN::restore(std::hint::black_box(&snapshots)))
                    })
                });
            }
            let mut aggregator = GroupBy::new(vec![0], vec![0], vec![1], vec![0]);
            aggregator.update(&input);
            let label = format!("group_snapshot/{rows}/keys={keys}");
            let (snapshot, allocations) = measure(|| aggregator.snapshot());
            assert!(!snapshot.is_empty());
            report(&label, input.columns(), &[], allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(aggregator.snapshot()))
            });
            let label = format!("group_restore/{rows}/keys={keys}");
            let (_, allocations) = measure(|| GroupBy::restore(&snapshot));
            report(&label, &[], &[], allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(GroupBy::restore(std::hint::black_box(&snapshot))))
            });
            let start: ArrayRef = Arc::new(Int64Array::from(vec![0; rows]));
            let end: ArrayRef = Arc::new(Int64Array::from(vec![1000; rows]));
            let window = RecordBatch::try_from_iter(vec![
                ("start", start),
                ("end", end),
                ("key", input.column(0).clone()),
                ("value", input.column(1).clone()),
            ])
            .unwrap();
            let label = format!("window_rank/{rows}/keys={keys}");
            let mut operator = WindowRank::new();
            let (output, allocations) = measure(|| operator.run(&window));
            assert!(output.num_rows() <= rows);
            report(&label, window.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched(
                    WindowRank::new,
                    |mut operator| std::hint::black_box(operator.run(&window)),
                    BatchSize::LargeInput,
                )
            });
        }
    }
    group.finish();
}
criterion_group!(benches, keys_and_checkpoints);
criterion_main!(benches);
