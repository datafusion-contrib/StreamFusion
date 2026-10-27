//! Release stock/previous/native integer rounding controls; native timers stay inside Rust.
use arrow::array::{Array, ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

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
        DataType::Int8 => sum!(Int8Array),
        DataType::Int16 => sum!(Int16Array),
        DataType::Int32 => sum!(Int32Array),
        DataType::Int64 => sum!(Int64Array),
        other => panic!("Unsupported integer rounding output {other}"),
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let function =
        std::env::var("SF_INTEGRAL_ROUNDING_FUNCTION").unwrap_or_else(|_| "FLOOR".into());
    assert!(function == "FLOOR");
    let function_option = format!("-Dsf.integral.rounding.function={function}");
    let element = args.get(1).map_or("INT", String::as_str);
    let nullable = args.get(2).is_some_and(|s| s == "true");
    let binary = args.get(3).is_some_and(|s| s == "true");
    let rows: usize = args.get(4).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(5).map_or(128, |s| s.parse().unwrap());
    let warmups: usize = args.get(6).map_or(12, |s| s.parse().unwrap());
    let runs: usize = args.get(7).map_or(9, |s| s.parse().unwrap());
    let only_engine = std::env::var("SF_NATIVE_BENCH_ENGINE").ok();
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
        .option(&function_option)
        .option("-Xmx1g")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/IntegralRoundingBenchmarkFunctions";
    let ty = env.new_string(element).unwrap();
    let id = java!(
        env,
        env.call_static_method(
            class,
            "register",
            "(Ljava/lang/String;)I",
            &[JValue::Object(&ty)]
        )
    )
    .i()
    .unwrap();
    let schema = java!(
        env,
        env.call_static_method(class, "schema", "()Ljava/lang/String;", &[])
    )
    .l()
    .unwrap();
    let schema: String = env
        .get_string(&jni::objects::JString::from(schema))
        .unwrap()
        .into();
    java!(
        env,
        env.call_static_method(
            class,
            "prepare",
            "(Ljava/lang/String;ZZI)V",
            &[
                JValue::Object(&ty),
                JValue::Bool(nullable.into()),
                JValue::Bool(binary.into()),
                JValue::Int(rows as i32)
            ]
        )
    );
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
            _ => panic!("Unsupported integer rounding input {element}"),
        }
    };
    let values = (0..rows).map(|i| (!nullable || i % 7 != 0).then(|| value(i)));
    let input: ArrayRef = match element {
        "TINYINT" => Arc::new(Int8Array::from_iter(values.map(|v| v.map(|v| v as i8)))),
        "SMALLINT" => Arc::new(Int16Array::from_iter(values.map(|v| v.map(|v| v as i16)))),
        "INT" => Arc::new(Int32Array::from_iter(values.map(|v| v.map(|v| v as i32)))),
        _ => Arc::new(Int64Array::from_iter(values)),
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            input.data_type().clone(),
            true,
        )])),
        vec![input],
    )
    .unwrap();
    let mut previous = CalcProgram::new(
        vec![38, 0],
        vec![0, 0],
        vec![1, 0],
        vec![i64::from(id), 0],
        vec![],
        vec![Some(schema)],
    );
    let mut native = CalcProgram::new(vec![0], vec![0], vec![0], vec![], vec![], vec![]);
    let expected = previous.run(batch.clone());
    let actual = native.run(batch.clone());
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.column(0), expected.column(0));
    let (diagnostic, allocations) = measure(|| native.run(batch.clone()));
    report(
        "integral_rounding",
        batch.columns(),
        diagnostic.columns(),
        allocations,
    );
    let expected = checksum(&expected).wrapping_mul(loops as i64);
    for trial in 0..warmups + runs {
        for engine in (0..3).map(|i| (i + trial) % 3) {
            let engine_name = ["flink", "previous", "native"][engine];
            if only_engine.as_ref().is_some_and(|name| name != engine_name) {
                continue;
            }
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
                let calc = if engine == 1 {
                    &mut previous
                } else {
                    &mut native
                };
                let start = Instant::now();
                let mut sum = 0i64;
                for _ in 0..loops {
                    sum =
                        sum.wrapping_add(checksum(&black_box(calc.run(black_box(batch.clone())))));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("integral_rounding,{function},{element},{nullable},{binary},{rows},{},{trial},{},{},{nanos},{digest}",
                rows*loops,trial<warmups,engine_name);
        }
    }
    java!(env, env.call_static_method(class, "close", "()V", &[]));
}
