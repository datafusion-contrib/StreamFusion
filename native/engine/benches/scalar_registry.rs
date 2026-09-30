//! Every registered Flink scalar kernel, with sliced nullable arrays and literal adaptation.
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, Date32Array, Decimal128Array, Float64Array, Int64Array, Int64Builder,
    ListBuilder, MapBuilder, StringArray,
};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use streamfusion::bench::{flink_scalar_function, parameterized_scalar, registered_scalar_codes};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::timestamps_from_millis;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn literal(value: &str) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.into())))
}
fn int(value: i32) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Int32(Some(value)))
}
fn long(value: i64) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Int64(Some(value)))
}
fn array(value: impl Array + 'static, rows: usize) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(value).slice(1, rows))
}

fn fixtures(rows: usize, nulls: bool, unicode: bool) -> Vec<(i64, Vec<ColumnarValue>)> {
    let valid = |row: usize| !nulls || row % 7 != 0;
    let text = if unicode {
        "é中🙂|alpha|beta".repeat(16)
    } else {
        "alpha|beta|ABC|".repeat(20)
    };
    let string = |value: &str| {
        array(
            StringArray::from_iter((0..rows + 1).map(|i| valid(i).then_some(value))),
            rows,
        )
    };
    let integers = array(
        Int64Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i64 - 10))),
        rows,
    );
    let dates = array(
        Date32Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i32 - 10))),
        rows,
    );
    let times: ColumnarValue = array(
        timestamps_from_millis(&Int64Array::from_iter(
            (0..rows + 1).map(|i| valid(i).then_some(i as i64 * 1000 - 10000)),
        )),
        rows,
    );
    let mut list = ListBuilder::new(Int64Builder::new());
    for row in 0..rows + 1 {
        for value in [
            Some(row as i64),
            None,
            Some(row as i64),
            Some(-(row as i64)),
        ] {
            list.values().append_option(value);
        }
        list.append(valid(row));
    }
    let mut cases = vec![
        (
            65,
            vec![array(
                Float64Array::from_iter((0..rows + 1).map(|i| {
                    valid(i).then_some(match i % 7 {
                        0 => -0.0,
                        1 => f64::NAN,
                        2 => f64::INFINITY,
                        _ => i as f64 - 100.0,
                    })
                })),
                rows,
            )],
        ),
        (67, vec![string(&text)]),
        (81, vec![integers.clone()]),
        (100, vec![string(&text), literal("alpha")]),
        (101, vec![string(&text), literal("beta")]),
        (102, vec![string(&text), literal("alpha")]),
        (103, vec![literal("alpha"), string(&text), int(1)]),
        (104, vec![integers.clone()]),
        (105, vec![integers.clone()]),
        (106, vec![string(&text)]),
        (107, vec![string(&text)]),
        (108, vec![string("00ff1234")]),
        (109, vec![integers.clone(), long(1)]),
        (110, vec![integers.clone(), long(1)]),
        (111, vec![string(&text)]),
        (112, vec![string(&text), literal("abc"), literal("ABC")]),
        (113, vec![string(&text)]),
        (114, vec![int(1), string(&text), literal("other")]),
        (115, vec![string(&text)]),
        (116, vec![string(&text), literal("ABC"), long(2), long(4)]),
        (117, vec![string("alpha%20beta+gamma")]),
        (118, vec![string("alpha%20beta+gamma")]),
        (120, vec![string(&text), literal("UTF-8")]),
        (
            121,
            vec![
                array(
                    BinaryArray::from_iter(
                        (0..rows + 1).map(|i| valid(i).then_some(text.as_bytes())),
                    ),
                    rows,
                ),
                literal("UTF-8"),
            ],
        ),
        (122, vec![string(&text)]),
        (123, vec![string("\"alpha\\n\"")]),
        (124, vec![string(&text), literal("|")]),
        (125, vec![string(&text), int(2), int(4)]),
        (126, vec![string(&text), int(4)]),
        (127, vec![string(&text), int(4)]),
        (128, vec![string(&text), int(320), literal(".")]),
        (129, vec![string(&text), int(320), literal(".")]),
        (130, vec![string(&text), literal("|"), int(1)]),
        (131, vec![string("2024-02-29")]),
        (133, vec![dates.clone()]),
        (134, vec![dates.clone()]),
        (135, vec![dates.clone()]),
        (136, vec![dates]),
        (139, vec![string(&text)]),
        (140, vec![string(&text)]),
        (
            142,
            vec![
                string("{\"a\":1}"),
                literal("strict $.a"),
                literal("FALSE"),
                literal("13.0"),
            ],
        ),
        (
            151,
            vec![array(
                BinaryArray::from_iter((0..rows + 1).map(|i| valid(i).then_some(text.as_bytes()))),
                rows,
            )],
        ),
        (152, vec![string(&text)]),
        (153, vec![literal("NULL"), literal("key"), string(&text)]),
        (
            154,
            vec![
                times.clone(),
                long(1000),
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ],
        ),
        (155, vec![times.clone()]),
        (156, vec![times.clone(), long(1000)]),
        (157, vec![times, int(1)]),
        (158, vec![integers.clone(), long(0)]),
        (159, vec![string("true")]),
        (160, vec![string("invalid")]),
        (161, vec![string(&text), literal("alpha"), int(1), int(1)]),
        (162, vec![string(&text)]),
        (165, vec![array(list.finish(), rows)]),
    ];
    for op in [144, 145, 146, 147] {
        cases.push((op, vec![string("{\"a\":1}"), literal("13.0")]));
    }
    for (op, document) in [
        (141, "{\"a\":\"text\"}"),
        (148, "{\"a\":true}"),
        (149, "{\"a\":123}"),
        (150, "{\"a\":12.5}"),
    ] {
        cases.push((
            op,
            vec![
                string(document),
                literal("strict $.a"),
                literal("NULL"),
                literal(""),
                literal("NULL"),
                literal(""),
                literal("13.0"),
            ],
        ));
    }
    let expected: BTreeSet<_> = registered_scalar_codes().into_iter().collect();
    assert_eq!(
        cases.iter().map(|(op, _)| *op).collect::<BTreeSet<_>>(),
        expected,
        "Every registered Flink scalar requires a benchmark fixture"
    );
    cases
}

fn parameterized(
    rows: usize,
    nulls: bool,
) -> Vec<(
    String,
    datafusion::logical_expr::ScalarUDF,
    Vec<ColumnarValue>,
)> {
    let valid = |i| !nulls || i % 7 != 0;
    let ints = array(
        Int64Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i64))),
        rows,
    );
    let decimals = array(
        Decimal128Array::from_iter((0..rows + 1).map(|i| valid(i).then_some(i as i128 * 12345)))
            .with_precision_and_scale(18, 2)
            .unwrap(),
        rows,
    );
    let floats = array(
        Float64Array::from_iter(
            (0..rows + 1).map(|i| valid(i).then_some(if i % 5 == 0 { f64::NAN } else { i as f64 })),
        ),
        rows,
    );
    let mut list = ListBuilder::new(Int64Builder::new());
    let mut map = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
    for i in 0..rows + 1 {
        for v in 0..4 {
            list.values().append_value(v);
            map.keys().append_value(v);
            map.values().append_option((v % 3 != 0).then_some(v));
        }
        list.append(valid(i));
        map.append(valid(i)).unwrap();
    }
    let list = array(list.finish(), rows);
    let map = array(map.finish(), rows);
    let mut cases = Vec::new();
    let mut add = |name: &str, ty: DataType, args: Vec<ColumnarValue>| {
        cases.push((name.to_string(), parameterized_scalar(name, ty), args))
    };
    for name in ["decimal_cast", "decimal_to_double", "decimal_to_float"] {
        add(name, decimals.data_type(), vec![decimals.clone()]);
    }
    for name in ["decimal_round", "decimal_truncate"] {
        add(name, decimals.data_type(), vec![decimals.clone(), int(1)]);
    }
    for name in ["decimal_add", "decimal_subtract", "decimal_multiply"] {
        add(
            name,
            decimals.data_type(),
            vec![decimals.clone(), decimals.clone()],
        );
    }
    add(
        "integer_divide",
        DataType::Int64,
        vec![ints.clone(), long(3)],
    );
    for name in ["integer_parse", "integer_try_parse"] {
        add(
            name,
            DataType::Int64,
            vec![array(
                StringArray::from_iter((0..rows + 1).map(|i| valid(i).then_some("12345678"))),
                rows,
            )],
        );
    }
    add("integer_format", DataType::Int64, vec![ints.clone()]);
    add("from_unixtime", DataType::Int64, vec![ints.clone()]);
    add("array_item", list.data_type(), vec![list, long(2)]);
    add("map_lookup_literal", map.data_type(), vec![map.clone()]);
    add("map_lookup_dynamic", map.data_type(), vec![map, long(1)]);
    add(
        "float_comparison",
        DataType::Float64,
        vec![floats.clone(), floats],
    );
    add("random", DataType::Float64, vec![]);
    add("random_seeded", DataType::Float64, vec![int(42)]);
    add("random_integer", DataType::Int32, vec![int(100)]);
    add(
        "random_integer_seeded",
        DataType::Int32,
        vec![int(42), int(100)],
    );
    for name in [
        "current_timestamp",
        "current_date",
        "current_time",
        "unix_timestamp",
        "watermark",
    ] {
        add(name, DataType::Int64, vec![]);
    }
    cases
}

fn registry(c: &mut Criterion) {
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH")
        .expect("Run bin/bench-native.py to prepare the SQL/JSON JVM classpath");
    let property = format!("-Djava.class.path={classpath}");
    let arguments = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&property)
        .option("-Xms128m")
        .option("-Xmx256m")
        .option("-Dfile.encoding=UTF-8")
        .option("-Duser.timezone=UTC")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(arguments).expect("Create the benchmark JVM");
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    header();
    let mut group = c.benchmark_group("scalar_registry");
    for rows in [16, 1024, 16384] {
        for (nulls, unicode) in [(false, false), (true, false), (true, true)] {
            group.throughput(Throughput::Elements(rows as u64));
            let mut cases: Vec<_> = fixtures(rows, nulls, unicode)
                .into_iter()
                .map(|(op, args)| {
                    (
                        format!("op={op}"),
                        flink_scalar_function(op, args.len()),
                        args,
                    )
                })
                .collect();
            if !unicode {
                cases.extend(parameterized(rows, nulls));
            }
            for (case, udf, args) in cases {
                let types: Vec<_> = args.iter().map(ColumnarValue::data_type).collect();
                let fields: Vec<_> = types
                    .iter()
                    .map(|ty| Arc::new(Field::new("arg", ty.clone(), true)))
                    .collect();
                let scalars: Vec<_> = args
                    .iter()
                    .map(|arg| match arg {
                        ColumnarValue::Scalar(value) => Some(value),
                        _ => None,
                    })
                    .collect();
                let field = udf
                    .return_field_from_args(ReturnFieldArgs {
                        arg_fields: &fields,
                        scalar_arguments: &scalars,
                    })
                    .unwrap();
                let output_type = field.data_type().clone();
                let options = Arc::new(ConfigOptions::new());
                let run = || {
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: fields.clone(),
                        number_rows: rows,
                        return_field: field.clone(),
                        config_options: options.clone(),
                    })
                    .unwrap()
                };
                let label = format!(
                    "{case}/{}/{rows}/nulls={nulls}/unicode={unicode}",
                    udf.name()
                );
                run();
                let (output, allocations) = measure(run);
                let output = output.to_array(rows).unwrap();
                assert_eq!(output.len(), rows);
                assert_eq!(output.data_type(), &output_type);
                let inputs: Vec<_> = args
                    .iter()
                    .filter_map(|arg| match arg {
                        ColumnarValue::Array(array) => Some(array.clone()),
                        _ => None,
                    })
                    .collect();
                report(&label, &inputs, &[output], allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(run()))
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, registry);
criterion_main!(benches);
