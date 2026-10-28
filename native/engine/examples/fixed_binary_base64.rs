//! Release stock/previous/native fixed-binary TO_BASE64 controls; native timers stay inside Rust.
use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray};
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
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .fold(0i64, |sum, v| {
            sum.wrapping_add(v.map_or(-1, |v| {
                let bytes = v.as_bytes();
                (bytes.len() + bytes[0] as usize + bytes[bytes.len() - 1] as usize) as i64
            }))
        })
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let width: i32 = args.get(1).map_or(16, |s| s.parse().unwrap());
    let nullable = args.get(2).is_some_and(|s| s == "true");
    let binary = args.get(3).is_some_and(|s| s == "true");
    let fixed = args.get(4).is_some_and(|s| s == "true");
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
        .option("-Xmx1g")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/FixedBinaryBase64BenchmarkFunctions";
    let id = java!(
        env,
        env.call_static_method(class, "register", "(I)I", &[JValue::Int(width)])
    )
    .i()
    .unwrap();
    java!(
        env,
        env.call_static_method(
            class,
            "prepare",
            "(IZZI)V",
            &[
                JValue::Int(width),
                JValue::Bool(nullable.into()),
                JValue::Bool(binary.into()),
                JValue::Int(rows as i32)
            ]
        )
    );
    let values: Vec<Option<Vec<u8>>> = (0..rows)
        .map(|i| {
            (!nullable || i % 7 != 0).then(|| {
                (0..width as usize)
                    .map(|j| (i * 131 + j * 127) as u8)
                    .collect()
            })
        })
        .collect();
    let input: ArrayRef = if fixed {
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                values.iter().map(|v| v.as_deref()),
                width,
            )
            .unwrap(),
        )
    } else {
        Arc::new(BinaryArray::from_iter(values.iter().map(|v| v.as_deref())))
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
        vec![17, 0],
        vec![0, 0],
        vec![1, 0],
        vec![i64::from(id), 0],
        vec![],
        vec![],
    );
    let mut native = CalcProgram::new(vec![6, 0], vec![151, 0], vec![1, 0], vec![], vec![], vec![]);
    let expected = previous.run(batch.clone());
    let actual = native.run(batch.clone());
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.column(0), expected.column(0));
    let (diagnostic, allocations) = measure(|| native.run(batch.clone()));
    report(
        "fixed_binary_base64",
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
            println!("fixed_binary_base64,{width},{nullable},{binary},{fixed},{rows},{},{trial},{},{},{nanos},{digest}",
                rows*loops,trial<warmups,engine_name);
        }
    }
    java!(env, env.call_static_method(class, "close", "()V", &[]));
}
