//! Release stock/previous/native FROM_BASE64 controls; native timers stay inside Rust.
use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
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
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .fold(0i64, |sum, v| {
            sum.wrapping_add(v.map_or(-1, |v| {
                let bytes = v;
                (bytes.len()
                    + if bytes.is_empty() {
                        0
                    } else {
                        bytes[0] as usize + bytes[bytes.len() - 1] as usize
                    }) as i64
            }))
        })
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|s| s == "--oracle") {
        oracle(&args[2]);
        return;
    }
    let width: i32 = args.get(1).map_or(16, |s| s.parse().unwrap());
    let nullable = args.get(2).is_some_and(|s| s == "true");
    let binary = args.get(3).is_some_and(|s| s == "true");
    let binary_row = args.get(4).is_some_and(|s| s == "true");
    let rows: usize = args.get(5).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(6).map_or(128, |s| s.parse().unwrap());
    let warmups: usize = args.get(7).map_or(12, |s| s.parse().unwrap());
    let runs: usize = args.get(8).map_or(9, |s| s.parse().unwrap());
    let previous = std::env::var("SF_FROM_BASE64_PREVIOUS").is_ok();
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
    let class = "tech/streamfusion/planner/FromBase64BenchmarkFunctions";
    let program = java!(
        env,
        env.call_static_method(class, "register", "(Z)[J", &[JValue::Bool(binary.into())])
    )
    .l()
    .unwrap();
    let program = jni::objects::JLongArray::from(program);
    let mut encoded = vec![0; env.get_array_length(&program).unwrap() as usize];
    env.get_long_array_region(&program, 0, &mut encoded)
        .unwrap();
    assert_eq!(
        encoded[0],
        if previous {
            if binary {
                17
            } else {
                44
            }
        } else {
            6
        }
    );
    if !previous {
        assert_eq!(encoded[1], 232);
    }
    java!(
        env,
        env.call_static_method(
            class,
            "prepare",
            "(IZZZI)V",
            &[
                JValue::Int(width),
                JValue::Bool(nullable.into()),
                JValue::Bool(binary.into()),
                JValue::Bool(binary_row.into()),
                JValue::Int(rows as i32)
            ]
        )
    );
    let values: Vec<Option<Vec<u8>>> = (0..rows)
        .map(|i| {
            (!nullable || i % 7 != 0).then(|| {
                use base64::Engine;
                let raw: Vec<u8> = (0..width as usize)
                    .map(|j| (i * 131 + j * 127) as u8)
                    .collect();
                base64::engine::general_purpose::STANDARD
                    .encode(raw)
                    .into_bytes()
            })
        })
        .collect();
    let bytes = BinaryArray::from_iter(values.iter().map(|v| v.as_deref()));
    let input: ArrayRef = if binary {
        Arc::new(bytes)
    } else {
        Arc::new(StringArray::new(
            bytes.offsets().clone(),
            bytes.values().clone(),
            bytes.nulls().cloned(),
        ))
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
    let mut calc = CalcProgram::new(
        vec![encoded[0], 0],
        vec![encoded[1], 0],
        vec![1, 0],
        encoded[2..].to_vec(),
        vec![],
        vec![],
    );
    let actual = calc.run(batch.clone());
    let (diagnostic, allocations) = measure(|| calc.run(batch.clone()));
    report(
        "from_base64",
        batch.columns(),
        diagnostic.columns(),
        allocations,
    );
    let expected = checksum(&actual).wrapping_mul(loops as i64);
    for trial in 0..warmups + runs {
        for engine in (0..2).map(|i| (i + trial) % 2) {
            let engine_name = if engine == 0 {
                "flink"
            } else if previous {
                "previous"
            } else {
                "native"
            };
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
                let start = Instant::now();
                let mut sum = 0i64;
                for _ in 0..loops {
                    sum =
                        sum.wrapping_add(checksum(&black_box(calc.run(black_box(batch.clone())))));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("from_base64,{width},{nullable},{binary},{binary_row},{rows},{},{trial},{},{},{nanos},{digest}",
                rows*loops,trial<warmups,engine_name);
        }
    }
    java!(env, env.call_static_method(class, "close", "()V", &[]));
}

fn oracle(path: &str) {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
    let function = streamfusion::bench::flink_scalar_function(232, 1);
    let mut count = 0;
    for line in std::fs::read_to_string(path).unwrap().lines() {
        let (input, expected) = line.split_once('\t').unwrap();
        let bytes: Vec<u8> = (0..input.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&input[i..i + 2], 16).unwrap())
            .collect();
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes)))],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("out", arrow::datatypes::DataType::Binary, true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        });
        let actual = match result {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes)))) => format!(
                "ok:{}",
                bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            ),
            Err(datafusion::common::DataFusionError::External(error)) => format!("err:{error}"),
            other => panic!("Unexpected oracle result: {other:?}"),
        };
        assert_eq!(actual, expected, "input={input}");
        count += 1;
    }
    println!("{count} released-JDK oracle cases passed");
}
