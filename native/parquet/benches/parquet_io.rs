use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
use streamfusion_parquet::bench::{decode, encode, encode_with_int96};
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
fn int96(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/parquet/int96");
    for rows in [16, 1024, 16384] {
        for nullable in [false, true] {
            let dates = [-62_135_596_800_000, -1, 0, 1, 253_402_300_799_999];
            let input_for = |wire_readable| {
                RecordBatch::try_from_iter(vec![
                    (
                        "id",
                        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                    ),
                    (
                        "ts",
                        Arc::new(timestamp_array((0..rows).map(|i| {
                            if nullable && i % 7 == 0 {
                                None
                            } else {
                                Some(
                                    TimestampValue::new(
                                        dates[i % dates.len()],
                                        if wire_readable && dates[i % dates.len()] == -1 {
                                            0
                                        } else {
                                            [0, 1, 123_456, 999_999][i % 4]
                                        },
                                    )
                                    .unwrap(),
                                )
                            }
                        }))) as ArrayRef,
                    ),
                ])
                .unwrap()
            };
            let input = input_for(false);
            let encode_input = input_for(true);
            group.throughput(Throughput::Elements(rows as u64));
            let selected: Vec<_> = (0..rows).step_by(2).collect();
            for selection in [None, Some(selected.as_slice())] {
                let input = encode_input.clone();
                let label = format!(
                    "encode/{rows}/nulls={nullable}/selected={}",
                    selection.is_some()
                );
                let (encoded, allocations) = measure(|| encode_with_int96(&input, selection, true));
                report(&label, input.columns(), &[], allocations);
                let output = decode(&encoded, 17);
                let combined =
                    arrow::compute::concat_batches(&output[0].schema(), &output).unwrap();
                let expected = match selection {
                    None => input.clone(),
                    Some(indices) => {
                        let indices = arrow::array::UInt32Array::from_iter_values(
                            indices.iter().map(|&i| i as u32),
                        );
                        RecordBatch::try_new(
                            input.schema(),
                            input
                                .columns()
                                .iter()
                                .map(|column| arrow::compute::take(column, &indices, None).unwrap())
                                .collect(),
                        )
                        .unwrap()
                    }
                };
                assert_eq!(combined.num_rows(), expected.num_rows());
                assert_eq!(combined.columns(), expected.columns());
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(encode_with_int96(
                            std::hint::black_box(&input),
                            selection,
                            true,
                        ))
                    })
                });
            }
            let encoded = canonical_int96(&input);
            for batch_rows in [17, 1024] {
                let label = format!("decode/{rows}/nulls={nullable}/batch_rows={batch_rows}");
                let (output, allocations) = measure(|| decode(&encoded, batch_rows));
                let combined =
                    arrow::compute::concat_batches(&output[0].schema(), &output).unwrap();
                assert_eq!(combined.num_rows(), input.num_rows());
                assert_eq!(combined.columns(), input.columns());
                let columns: Vec<_> = output
                    .iter()
                    .flat_map(|batch| batch.columns().iter().cloned())
                    .collect();
                report(&label, &[], &columns, allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(decode(std::hint::black_box(&encoded), batch_rows))
                    })
                });
            }
        }
    }
    group.finish();
}
fn canonical_int96(input: &RecordBatch) -> Vec<u8> {
    use parquet::data_type::{Int64Type, Int96, Int96Type};
    use parquet::file::{properties::WriterProperties, writer::SerializedFileWriter};
    use streamfusion_bridge::timestamp::TimestampColumn;
    let schema = Arc::new(
        parquet::schema::parser::parse_message_type(
            "message fixture { REQUIRED INT64 id; OPTIONAL INT96 ts; }",
        )
        .unwrap(),
    );
    let mut bytes = Vec::new();
    let mut writer =
        SerializedFileWriter::new(&mut bytes, schema, Arc::new(WriterProperties::default()))
            .unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut id = group.next_column().unwrap().unwrap();
    let ids: Vec<_> = (0..input.num_rows() as i64).collect();
    id.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    id.close().unwrap();
    let ts = TimestampColumn::try_new(input.column(1).as_ref()).unwrap();
    let mut values = Vec::new();
    let mut definitions = Vec::new();
    for row in 0..input.num_rows() {
        definitions.push(i16::from(!ts.is_null(row)));
        if ts.is_null(row) {
            continue;
        }
        let nanos = ts.value(row).unwrap().nanos();
        let day = nanos.div_euclid(86_400_000_000_000) + 2_440_588;
        let fraction = nanos.rem_euclid(86_400_000_000_000) as u64;
        let mut value = Int96::new();
        value.set_data(fraction as u32, (fraction >> 32) as u32, day as u32);
        values.push(value);
    }
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int96Type>()
        .write_batch(&values, Some(&definitions), None)
        .unwrap();
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();
    bytes
}
criterion_group!(benches, parquet, int96);
criterion_main!(benches);
