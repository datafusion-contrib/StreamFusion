use arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Array, Int8Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampNanosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
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
fn nested_row_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/nested_row_keys");
    for rows in [16, 1024, 16384] {
        for keys in [1, 64, rows] {
            let fields: Fields = vec![
                Field::new("n", DataType::Int64, true),
                Field::new(
                    "t",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new("__sf_nested_row_kind", DataType::Int8, true)
                    .with_metadata([("streamfusion.nested-row-kind".into(), "1".into())].into()),
            ]
            .into();
            let column: ArrayRef = Arc::new(StructArray::new(
                fields,
                vec![
                    Arc::new(Int64Array::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some((i % keys) as i64)),
                    )),
                    Arc::new(TimestampNanosecondArray::from_iter(
                        (0..rows).map(|i| (i % 5 != 0).then_some(-123456789 + (i % keys) as i64)),
                    )),
                    Arc::new(Int8Array::from_iter(
                        (0..rows).map(|i| (i % 11 != 0).then_some((i % 4) as i8)),
                    )),
                ],
                Some(NullBuffer::from(
                    (0..rows).map(|i| i % 13 != 0).collect::<Vec<_>>(),
                )),
            ));
            let input = RecordBatch::try_from_iter(vec![("r", column.clone())]).unwrap();
            let descriptors = [-1, -1, 9];
            group.throughput(Throughput::Elements(rows as u64));
            let label = format!("flink_binary_row_hash/{rows}/keys={keys}");
            let (output, allocations) = measure(|| flink_key_hashes(&input, &[0], &descriptors));
            assert_eq!(output.len(), rows);
            report(
                &format!("engine/nested_row_keys/{label}"),
                input.columns(),
                &[],
                allocations,
            );
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| {
                    std::hint::black_box(flink_key_hashes(
                        std::hint::black_box(&input),
                        &[0],
                        &descriptors,
                    ))
                })
            });
            let label = format!("ordering/{rows}/keys={keys}");
            let (output, allocations) =
                measure(|| streamfusion_bridge::ordering::canonical_ordering_column(&column));
            assert_eq!(output.data_type(), column.data_type());
            let kinds = output
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            assert!(kinds.iter().all(|kind| kind == Some(0)));
            report(
                &format!("engine/nested_row_keys/{label}"),
                &[column.clone()],
                &[output],
                allocations,
            );
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| {
                    std::hint::black_box(streamfusion_bridge::ordering::canonical_ordering_column(
                        std::hint::black_box(&column),
                    ))
                })
            });
            if keys == 1 {
                let mut offsets = vec![0i32];
                for i in 0..rows {
                    offsets.push(offsets[i] + i32::from(i % 5 != 0));
                }
                let lists: ArrayRef = Arc::new(ListArray::new(
                    Arc::new(Field::new("item", column.data_type().clone(), true)),
                    OffsetBuffer::new(offsets.into()),
                    column.clone(),
                    Some(NullBuffer::from(
                        (0..rows).map(|i| i % 7 != 0).collect::<Vec<_>>(),
                    )),
                ));
                let input = RecordBatch::try_from_iter(vec![
                    (
                        "id",
                        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                    ),
                    ("items", lists),
                ])
                .unwrap();
                for left in [false, true] {
                    let label = format!("unnest/{rows}/left={left}");
                    let (output, allocations) =
                        measure(|| streamfusion::bench::unnest(&input, left, true));
                    assert_eq!(
                        output.num_columns(),
                        5,
                        "metadata must not become a SQL column"
                    );
                    report(
                        &format!("engine/nested_row_keys/{label}"),
                        input.columns(),
                        output.columns(),
                        allocations,
                    );
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(streamfusion::bench::unnest(
                                std::hint::black_box(&input),
                                left,
                                true,
                            ))
                        })
                    });
                }
            }
        }
    }
    group.finish();
}
fn nested_binary_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/nested_binary_keys");
    for rows in [16_usize, 1024, 16384] {
        for width in [4, 256] {
            for seeds in [1, 64, rows] {
                let fields: Fields = vec![
                    Field::new("payload", DataType::Binary, true),
                    Field::new("__sf_nested_row_kind", DataType::Int8, true).with_metadata(
                        [("streamfusion.nested-row-kind".into(), "1".into())].into(),
                    ),
                ]
                .into();
                let payloads: Vec<Vec<u8>> = (0..rows + 1)
                    .map(|i| {
                        let length = match i % 4 {
                            0 => 0,
                            1 => 1,
                            2 => width,
                            _ => width + 3,
                        };
                        let seed = (i % seeds).to_le_bytes();
                        (0..length).map(|j| seed[j % seed.len()]).collect()
                    })
                    .collect();
                let column: ArrayRef = Arc::new(StructArray::new(
                    fields,
                    vec![
                        Arc::new(BinaryArray::from_iter(
                            payloads
                                .iter()
                                .enumerate()
                                .map(|(i, bytes)| (i % 7 != 0).then_some(bytes.as_slice())),
                        )),
                        Arc::new(Int8Array::from_iter_values(
                            (0..rows + 1).map(|i| (i % 4) as i8),
                        )),
                    ],
                    Some(NullBuffer::from(
                        (0..rows + 1).map(|i| i % 13 != 0).collect::<Vec<_>>(),
                    )),
                ));
                let batch = RecordBatch::try_from_iter(vec![("r", column)])
                    .unwrap()
                    .slice(1, rows);
                let descriptors = [-1, -1];
                let label = format!("{rows}/declared_width=4/payload_width={width}/seeds={seeds}");
                group.throughput(Throughput::Elements(rows as u64));
                let (hashes, allocations) =
                    measure(|| flink_key_hashes(&batch, &[0], &descriptors));
                assert_eq!(hashes.len(), rows);
                report(
                    &format!("engine/nested_binary_keys/{label}"),
                    batch.columns(),
                    &[],
                    allocations,
                );
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(flink_key_hashes(
                            std::hint::black_box(&batch),
                            &[0],
                            &descriptors,
                        ))
                    })
                });
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    keys_and_checkpoints,
    nested_row_keys,
    nested_binary_keys
);
criterion_main!(benches);
