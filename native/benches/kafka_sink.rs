//! Native Kafka sink serialization: one writer over a whole Arrow batch versus invoking that same
//! production encoder once per row, the batching win isolated from Kafka I/O and checkpoints.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use streamfusion::bench::encode_kafka_json;

const ROWS: usize = 4096;

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("price", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)) as ArrayRef,
            Arc::new(StringArray::from_iter(
                (0..ROWS).map(|row| Some(format!("auction-{row}"))),
            )),
            Arc::new(BooleanArray::from_iter(
                (0..ROWS).map(|row| Some(row % 2 == 0)),
            )),
            Arc::new(Float64Array::from_iter_values(
                (0..ROWS).map(|row| row as f64 + 0.25),
            )),
        ],
    )
    .unwrap()
}

fn bench_kafka_json(c: &mut Criterion) {
    let batch = batch();
    let mut group = c.benchmark_group("kafka_json_sink");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("whole_arrow_batch", |b| {
        b.iter(|| black_box(encode_kafka_json(black_box(&batch))))
    });
    group.bench_function("one_writer_per_row", |b| {
        b.iter(|| {
            let mut encoded = Vec::with_capacity(ROWS);
            for row in 0..ROWS {
                encoded.push(encode_kafka_json(&batch.slice(row, 1)).pop().unwrap());
            }
            black_box(encoded)
        })
    });
    group.finish();
}

criterion_group!(benches, bench_kafka_json);
criterion_main!(benches);
