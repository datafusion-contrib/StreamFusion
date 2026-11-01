//! Release stock/previous/native integer-expression controls; native timers stay inside Rust.
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(serde::Deserialize)]
struct EncodedProgram {
    kinds: Vec<i64>,
    payload: Vec<i64>,
    child_counts: Vec<i64>,
    longs: Vec<i64>,
    doubles: Vec<f64>,
    strings: Vec<Option<String>>,
    boolean_output: bool,
    #[serde(default)]
    string_output: bool,
}

macro_rules! java {
    ($env:ident, $call:expr) => {{
        let result = $call;
        if result.is_err() {
            let _ = $env.exception_describe();
        }
        result.expect("Java benchmark control failed")
    }};
}

fn checksum(batch: &RecordBatch) -> i64 {
    macro_rules! sum {
        ($ty:ty) => {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .iter()
                .fold(0i64, |sum, v| sum.wrapping_add(v.map_or(-1, i64::from)))
        };
    }
    match batch.column(0).data_type() {
        DataType::Boolean => sum!(BooleanArray),
        DataType::Int8 => sum!(Int8Array),
        DataType::Int16 => sum!(Int16Array),
        DataType::Int32 => sum!(Int32Array),
        DataType::Int64 => sum!(Int64Array),
        DataType::Utf8 => batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| {
                value.map_or(-1, |value| {
                    value.len() as i64
                        + value.as_bytes().first().map_or(0, |v| i64::from(*v))
                        + value.as_bytes().last().map_or(0, |v| i64::from(*v))
                })
            })
            .sum(),
        other => panic!("Unsupported exact-numeric-expression output {other}"),
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let element = args.get(1).map_or("INT", String::as_str);
    let nullable = args.get(2).is_some_and(|s| s == "true");
    let binary = args.get(3).is_some_and(|s| s == "true");
    let rows: usize = args.get(4).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(5).map_or(128, |s| s.parse().unwrap());
    let warmups: usize = args.get(6).map_or(12, |s| s.parse().unwrap());
    let runs: usize = args.get(7).map_or(9, |s| s.parse().unwrap());
    let previous = std::env::var("SF_INTEGRAL_PREVIOUS").is_ok();
    let expression = std::env::var("SF_INTEGRAL_EXPRESSION")
        .unwrap_or_else(|_| "ABS(v+CAST(1 AS $TYPE))".into());
    let previous_option = format!("-Dsf.integral.previous={previous}");
    let expression_option = format!("-Dsf.integral.expression={expression}");
    let legacy_option = format!(
        "-Dsf.integral.legacy={}",
        std::env::var("SF_INTEGRAL_LEGACY").is_ok()
    );
    let cp = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let lib = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap_or_default()
    );
    let options = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&cp)
        .option(&lib)
        .option(&previous_option)
        .option(&expression_option)
        .option(&legacy_option)
        .option("-Xmx1g")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/IntegralExpressionBenchmarkFunctions";
    let ty = env.new_string(element).unwrap();
    let program = java!(
        env,
        env.call_static_method(
            class,
            "register",
            "(Ljava/lang/String;ZZI)Ljava/lang/String;",
            &[
                JValue::Object(&ty),
                JValue::Bool(nullable.into()),
                JValue::Bool(binary.into()),
                JValue::Int(rows as i32)
            ]
        )
    )
    .l()
    .unwrap();
    let program: String = env
        .get_string(&jni::objects::JString::from(program))
        .unwrap()
        .into();
    let program: EncodedProgram = serde_json::from_str(&program).unwrap();
    let value = |i: usize| {
        let v = (i as i64) * 7919;
        match element {
            "TINYINT" => i64::from(if i % 17 == 0 { i8::MIN } else { v as i8 }),
            "SMALLINT" => i64::from(if i % 17 == 0 { i16::MIN } else { v as i16 }),
            "INT" => {
                if i % 17 == 0 {
                    i64::from(i32::MIN)
                } else {
                    (if i % 2 == 0 { -v } else { v }) as i32 as i64
                }
            }
            "BIGINT" => {
                if i % 17 == 0 {
                    i64::MIN
                } else {
                    (if i % 2 == 0 { -1 } else { 1 }) * (9_007_199_254_740_993 + v)
                }
            }
            _ => panic!("Unsupported integer-expression input {element}"),
        }
    };
    let values = (0..rows).map(|i| (!nullable || i % 7 != 0).then(|| value(i)));
    let input: ArrayRef = match element {
        "BOOLEAN" => Arc::new(BooleanArray::from_iter(
            (0..rows).map(|i| (!nullable || i % 7 != 0).then_some(i % 2 == 0)),
        )),
        "TINYINT" => Arc::new(Int8Array::from_iter(values.map(|v| v.map(|v| v as i8)))),
        "SMALLINT" => Arc::new(Int16Array::from_iter(values.map(|v| v.map(|v| v as i16)))),
        "INT" => Arc::new(Int32Array::from_iter(values.map(|v| v.map(|v| v as i32)))),
        "BIGINT" => Arc::new(Int64Array::from_iter(values)),
        "DECIMAL(18,2)" | "DECIMAL(38,18)" => {
            let (precision, scale) = if element == "DECIMAL(18,2)" {
                (18, 2)
            } else {
                (38, 18)
            };
            let maximum = 10_i128.pow(precision as u32) - 1;
            let values = (0..rows).map(|row| {
                (!nullable || row % 7 != 0).then(|| match row % 17 {
                    0 => maximum,
                    1 => -maximum,
                    _ => {
                        (if row % 2 == 0 { -1 } else { 1 })
                            * (9_007_199_254_740_993 + row as i128 * 7919)
                    }
                })
            });
            Arc::new(
                Decimal128Array::from_iter(values)
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            )
        }
        _ => panic!("Unsupported exact-numeric-expression input {element}"),
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            input.data_type().clone(),
            nullable,
        )])),
        vec![input],
    )
    .unwrap();
    let mut native = CalcProgram::new(
        program.kinds,
        program.payload,
        program.child_counts,
        program.longs,
        program.doubles,
        program.strings,
    );
    let actual = native.run(batch.clone());
    assert_eq!(actual.num_rows(), rows);
    assert_eq!(
        actual.column(0).data_type(),
        if program.string_output {
            &DataType::Utf8
        } else if program.boolean_output {
            &DataType::Boolean
        } else {
            batch.column(0).data_type()
        }
    );
    let expected = checksum(&actual).wrapping_mul(loops as i64);
    let (diagnostic, allocations) = measure(|| native.run(batch.clone()));
    report(
        "integral_expression",
        batch.columns(),
        diagnostic.columns(),
        allocations,
    );
    for trial in 0..warmups + runs {
        for engine in (0..2).map(|i| (i + trial) % 2) {
            let (nanos, digest) = if engine == 0 {
                let result = java!(
                    env,
                    env.call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                )
                .l()
                .unwrap();
                let mut pair = [0; 2];
                env.get_long_array_region(jni::objects::JLongArray::from(result), 0, &mut pair)
                    .unwrap();
                (pair[0] as u128, pair[1])
            } else {
                let calc = &mut native;
                let start = Instant::now();
                let mut sum = 0i64;
                for _ in 0..loops {
                    sum =
                        sum.wrapping_add(checksum(&black_box(calc.run(black_box(batch.clone())))));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("integral_expression,{element},{nullable},{binary},{rows},{},{trial},{},{},{nanos},{digest}",
                rows*loops,trial<warmups,
                if engine == 0 { "flink" } else if previous { "previous" } else { "native" });
        }
    }
    java!(env, env.call_static_method(class, "close", "()V", &[]));
}
