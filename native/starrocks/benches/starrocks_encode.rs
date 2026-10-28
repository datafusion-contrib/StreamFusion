use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::timestamps_from_millis;
use streamfusion_starrocks::{Encoder, Format, SLAB_BYTES};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const TYPES: [&str; 9] = [
    "BIGINT",
    "INT",
    "STRING",
    "DECIMAL(18, 2)",
    "DOUBLE",
    "DATE",
    "TIMESTAMP(3)",
    "BOOLEAN",
    "STRING",
];
const NAMES: [&str; 9] = [
    "id",
    "quantity",
    "name",
    "amount",
    "score",
    "day",
    "event_time",
    "active",
    "comment",
];

fn input(rows: usize, upsert: bool) -> RecordBatch {
    let comment = "Unicode é and a longer payload that resembles a real description field";
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("quantity", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(18, 2), true),
        Field::new("score", DataType::Float64, true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "event_time",
            streamfusion_bridge::timestamp::timestamp_type(),
            true,
        ),
        Field::new("active", DataType::Boolean, true),
        Field::new("comment", DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        Arc::new(Int32Array::from_iter(
            (0..rows).map(|row| (row % 11 != 0).then_some(row as i32 * 7)),
        )),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|row| (row % 13 != 0).then(|| format!("customer-{row}"))),
        )),
        Arc::new(
            Decimal128Array::from_iter((0..rows).map(|row| Some(row as i128 * 1_234 - 50_000)))
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ),
        Arc::new(Float64Array::from_iter_values(
            (0..rows).map(|row| row as f64 / 3.0),
        )),
        Arc::new(Date32Array::from_iter_values(
            (0..rows).map(|row| 19_000 + (row % 400) as i32),
        )),
        Arc::new(timestamps_from_millis(&Int64Array::from_iter_values(
            (0..rows).map(|row| 1_700_000_000_000 + row as i64 * 1_337),
        ))),
        Arc::new(BooleanArray::from_iter(
            (0..rows).map(|row| Some(row % 2 == 0)),
        )),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|row| (row % 3 != 0).then_some(comment)),
        )),
    ];
    if upsert {
        fields.push(Field::new("$row_kind$", DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from_iter_values(
            (0..rows).map(|row| [0, 1, 2, 3][row % 4]),
        )));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn encoder(json: bool, upsert: bool) -> Encoder {
    let format = if json {
        Format::Json
    } else {
        Format::Csv {
            column_separator: b"\t".to_vec(),
        }
    };
    // The connector's JSON key order for these field names: the iteration order of its row
    // HashMap, as StarRocksEncoding.jsonOrder computes it, with -1 for `__op`.
    let order: &[i32] = if upsert {
        &[4, 3, 1, 2, 7, 8, 0, 5, -1, 6]
    } else {
        &[4, 3, 1, 2, 7, 8, 0, 5, 6]
    };
    Encoder::new(
        format,
        &TYPES.map(String::from),
        &NAMES.map(String::from),
        order,
        upsert,
        true,
    )
    .unwrap()
}

/// Every row of a batch in production-sized slabs, reusing the task's output buffers.
fn encode_batch(
    encoder: &Encoder,
    input: &RecordBatch,
    output: &mut Vec<u8>,
    ends: &mut Vec<usize>,
) -> usize {
    let (mut row, mut rows) = (0, 0);
    while row < input.num_rows() {
        output.clear();
        ends.clear();
        row = encoder
            .encode(input, row, SLAB_BYTES, usize::MAX, output, ends)
            .unwrap();
        rows += ends.len();
        std::hint::black_box(&output);
    }
    rows
}

fn encode(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("starrocks/encode");
    for rows in [1024, 8192] {
        for json in [false, true] {
            for upsert in [false, true] {
                let input = input(rows, upsert);
                let encoder = encoder(json, upsert);
                let label = format!(
                    "{}/{}/{rows}",
                    if json { "json" } else { "csv" },
                    if upsert { "upsert" } else { "append" }
                );
                let (mut output, mut ends) = (Vec::new(), Vec::new());
                encode_batch(&encoder, &input, &mut output, &mut ends);
                let (encoded, allocations) =
                    measure(|| encode_batch(&encoder, &input, &mut output, &mut ends));
                // The default sink.ignore.update-before drops every fourth row, the -U images.
                assert_eq!(encoded, if upsert { rows - rows / 4 } else { rows });
                report(&label, input.columns(), &[], allocations);
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        encode_batch(
                            &encoder,
                            std::hint::black_box(&input),
                            &mut output,
                            &mut ends,
                        )
                    })
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, encode);
criterion_main!(benches);
