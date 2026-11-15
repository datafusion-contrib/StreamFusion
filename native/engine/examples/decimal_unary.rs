//! Release stock/previous/native decimal unary controls; native timers stay inside Rust.
use arrow::array::{Array, ArrayRef, Decimal128Array};
use arrow::datatypes::{Field, Schema};
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
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .iter()
        .fold(0i64, |sum, v| {
            sum.wrapping_add(v.map_or(-1, |v| v.signum() as i64))
        })
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let function = std::env::var("SF_DECIMAL_UNARY_FUNCTION").unwrap_or_else(|_| "ABS".into());
    assert!(matches!(function.as_str(), "ABS" | "NEGATE"));
    let function_option = format!("-Dsf.decimal.unary.function={function}");
    let precision: u8 = args.get(1).map_or(18, |s| s.parse().unwrap());
    let scale: i8 = args.get(2).map_or(3, |s| s.parse().unwrap());
    let nullable = args.get(3).is_some_and(|s| s == "true");
    let binary = args.get(4).is_some_and(|s| s == "true");
    let rows: usize = args.get(5).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(6).map_or(128, |s| s.parse().unwrap());
    let warmups: usize = args.get(7).map_or(12, |s| s.parse().unwrap());
    let runs: usize = args.get(8).map_or(9, |s| s.parse().unwrap());
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
    let class = "tech/streamfusion/planner/DecimalUnaryBenchmarkFunctions";
    let id = java!(
        env,
        env.call_static_method(
            class,
            "register",
            "(II)I",
            &[
                JValue::Int(i32::from(precision)),
                JValue::Int(i32::from(scale))
            ]
        )
    )
    .i()
    .unwrap();
    let result_code = java!(env, env.call_static_method(class, "resultCode", "()I", &[]))
        .i()
        .unwrap();
    java!(
        env,
        env.call_static_method(
            class,
            "prepare",
            "(IIZZI)V",
            &[
                JValue::Int(i32::from(precision)),
                JValue::Int(i32::from(scale)),
                JValue::Bool(nullable.into()),
                JValue::Bool(binary.into()),
                JValue::Int(rows as i32)
            ]
        )
    );
    let limit = 10i128.pow(u32::from(precision));
    let values = (0..rows).map(|i| {
        let value = if i % 17 == 0 {
            limit - 1
        } else {
            ((if precision > 18 {
                9_007_199_254_740_993
            } else {
                0
            }) + i as i128 * 7919
                + 1)
                % limit
        };
        (!nullable || i % 7 != 0).then_some(if i % 2 == 0 { -value } else { value })
    });
    let input: ArrayRef = Arc::new(
        Decimal128Array::from_iter(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    );
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
        vec![17, 0],
        vec![0, 0],
        vec![1, 0],
        vec![i64::from(id), i64::from(result_code)],
        vec![],
        vec![],
    );
    let mut native = CalcProgram::new(
        vec![6, 0],
        vec![if function == "NEGATE" { 219 } else { 218 }, 0],
        vec![1, 0],
        vec![],
        vec![],
        vec![],
    );
    let expected = previous.run(batch.clone());
    let actual = native.run(batch.clone());
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.column(0), expected.column(0));
    let (diagnostic, allocations) = measure(|| native.run(batch.clone()));
    report(
        "decimal_unary",
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
            println!("decimal_unary,{function},{precision},{scale},{nullable},{binary},{rows},{},{trial},{},{},{nanos},{digest}",
                rows*loops,trial<warmups,engine_name);
        }
    }
    java!(env, env.call_static_method(class, "close", "()V", &[]));
}
