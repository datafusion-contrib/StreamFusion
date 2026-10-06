use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    TimestampNanosecondArray,
};
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
fn text_envelope(c: &mut Criterion) {
    let mut group = c.benchmark_group("format/csv/text_envelope");
    for rows in [16, 1024, 16384] {
        for unicode in [false, true] {
            let valid = if unicode {
                "٩٢٢٣٣٧٢٠٣٦٨٥٤٧٧٥٨٠٧,1.25d,2020-01-02 03:04:05.123456789"
            } else {
                "9223372036854775807,1.25d,2020-01-02 03:04:05.123456789"
            };
            let invalid = if unicode {
                "\u{a0}12,é,aaaaaaaaaéxx"
            } else {
                "overflow,NaNf,bad_timestamp"
            };
            let body: ArrayRef = Arc::new(BinaryArray::from_iter((0..rows).map(|row| {
                if row % 11 == 0 {
                    None
                } else {
                    Some(if row % 7 == 0 { invalid } else { valid }.as_bytes())
                }
            })));
            let batch = RecordBatch::try_from_iter(vec![("body", body)]).unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("score", DataType::Float64, true),
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
            ]));
            let decoder = CsvDecode::new(schema, true);
            let (output, allocations) = measure(|| decoder.decode(&batch));
            assert_eq!(
                output.num_rows(),
                rows - (0..rows).filter(|row| row % 11 == 0).count()
            );
            let ids = output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let scores = output
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let times = output
                .column(2)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            for (output_row, input_row) in (0..rows).filter(|row| row % 11 != 0).enumerate() {
                let invalid = input_row % 7 == 0;
                assert_eq!(ids.is_null(output_row), invalid);
                assert_eq!(scores.is_null(output_row), invalid);
                assert_eq!(times.is_null(output_row), invalid);
                if !invalid {
                    assert_eq!(ids.value(output_row), i64::MAX);
                    assert_eq!(scores.value(output_row), 1.25);
                    assert_eq!(times.value(output_row), 1_577_934_245_123_456_789);
                }
            }
            let label = format!("{rows}/unicode={unicode}");
            report(
                &format!("format/csv/text_envelope/{label}"),
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
    group.finish();
}
fn decimal_exponents(c: &mut Criterion) {
    let mut group = c.benchmark_group("format/csv/decimal_exponents");
    let examples = [
        ("0e2147483647", Some(0)),
        ("0e-2147483647", Some(0)),
        ("0e100000000", Some(0)),
        ("1e100000000", None),
        ("1e-100000000", Some(0)),
        ("1e2147483647", None),
        ("1e-2147483647", None),
        ("1.235", Some(124)),
        ("-1.235", Some(-124)),
        ("١.٢٣e٢", Some(12300)),
    ];
    for rows in [16, 1024, 16384] {
        let body: ArrayRef = Arc::new(BinaryArray::from_iter(
            (0..rows).map(|row| Some(examples[row % examples.len()].0.as_bytes())),
        ));
        let batch = RecordBatch::try_from_iter(vec![("body", body)]).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(5, 2),
            true,
        )]));
        let decoder = CsvDecode::new(schema, true);
        let (output, allocations) = measure(|| decoder.decode(&batch));
        let decimals = output
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(output.num_rows(), rows);
        for (row, actual) in decimals.iter().enumerate() {
            assert_eq!(actual, examples[row % examples.len()].1);
        }
        report(
            &format!("format/csv/decimal_exponents/{rows}"),
            batch.columns(),
            output.columns(),
            allocations,
        );
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::from_parameter(rows), |b| {
            b.iter(|| std::hint::black_box(decoder.decode(std::hint::black_box(&batch))))
        });
    }
    group.finish();
}
criterion_group!(benches, csv, text_envelope, decimal_exponents);
criterion_main!(benches);
