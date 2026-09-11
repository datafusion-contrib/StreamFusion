//! Per-function compute diagnostics. End-to-end Flink comparisons live in ScalarFunctionBenchmark.
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use streamfusion::bench::flink_scalar_function;

const ROWS: usize = 4096;

fn scalar_text(value: &str) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_owned())))
}

fn array(value: impl Array + 'static) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(value) as ArrayRef)
}

fn functions(c: &mut Criterion) {
    for (name, bytes, unicode, null_every) in [
        ("short", 8, false, 0),
        ("ascii", 264, false, 0),
        ("long", 4096, false, 0),
        ("unicode_nulls", 264, true, 8),
    ] {
        let payload = |pattern: &str| {
            pattern.repeat(bytes / pattern.len()) + &"x".repeat(bytes % pattern.len())
        };
        let patterns = if unicode {
            [" \u{4e2d}AbC \u{1f600}dEf ", " \u{e9}dEf \u{1f642}AbC "]
        } else {
            [" abC def_09 ", " dEf abc_90 "]
        };
        let text = patterns.map(payload);
        let valid = |row: usize| null_every == 0 || row % null_every != 0;
        let strings = |shift: usize| {
            array(StringArray::from_iter(
                (0..ROWS).map(|i| valid(i).then_some(text[(i + shift) % 2].as_str())),
            ))
        };
        let encoded = payload(if unicode {
            "%E4%B8%AD+%F0%9F%98%80"
        } else {
            "a+b%2B%20"
        });
        let numbers = || {
            vec![
                array(Int64Array::from_iter(
                    (0..ROWS).map(|i| valid(i).then_some(i as i64 - ROWS as i64 / 2)),
                )),
                array(Int64Array::from_iter_values(
                    (0..ROWS).map(|i| ROWS as i64 / 3 - i as i64),
                )),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(17))),
            ]
        };
        let cases = [
            ("GREATEST", 109, numbers()),
            ("LEAST", 110, numbers()),
            ("INITCAP", 111, vec![strings(0)]),
            (
                "TRANSLATE",
                112,
                vec![strings(0), scalar_text("abcdef"), scalar_text("ABCDEF")],
            ),
            ("BTRIM", 113, vec![strings(0)]),
            (
                "ELT",
                114,
                vec![
                    array(Int32Array::from_iter_values(
                        (0..ROWS).map(|i| (i % 2) as i32 + 1),
                    )),
                    strings(0),
                    strings(1),
                ],
            ),
            (
                "ELT_CONSTANT_INDEX",
                114,
                vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                    strings(0),
                    strings(1),
                ],
            ),
            ("URL_ENCODE", 115, vec![strings(0)]),
            (
                "OVERLAY",
                116,
                vec![
                    strings(0),
                    scalar_text("ABC"),
                    array(Int64Array::from_iter_values(
                        (0..ROWS).map(|i| (i % 2) as i64 + 1),
                    )),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
                ],
            ),
            (
                "URL_DECODE",
                117,
                vec![array(StringArray::from_iter(
                    (0..ROWS).map(|i| valid(i).then_some(encoded.as_str())),
                ))],
            ),
        ];
        let mut group = c.benchmark_group(format!("scalar/{name}"));
        group.throughput(Throughput::Elements(ROWS as u64));
        group.sample_size(20);
        group.warm_up_time(Duration::from_millis(300));
        group.measurement_time(Duration::from_secs(1));
        for (function, op, args) in cases {
            let udf = flink_scalar_function(op, args.len());
            let output_type = if op <= 110 {
                DataType::Int64
            } else {
                DataType::Utf8
            };
            let return_field = Arc::new(Field::new("result", output_type, true));
            let arg_fields: Vec<_> = args
                .iter()
                .map(|a| Arc::new(Field::new("arg", a.data_type(), true)))
                .collect();
            let options = Arc::new(ConfigOptions::new());
            group.bench_with_input(BenchmarkId::new(function, bytes), &args, |b, args| {
                b.iter(|| {
                    std::hint::black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: std::hint::black_box(args.clone()),
                            arg_fields: arg_fields.clone(),
                            number_rows: ROWS,
                            return_field: return_field.clone(),
                            config_options: options.clone(),
                        })
                        .unwrap(),
                    )
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, functions);
criterion_main!(benches);
