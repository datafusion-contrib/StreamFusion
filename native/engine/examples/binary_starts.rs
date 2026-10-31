//! Prepared binary STARTSWITH with production Calc and released generated RowData expressions.
use arrow::array::{ArrayRef, BinaryArray, BooleanArray};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

fn checksum(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .map(|s| s.map_or(-1, |s| i64::from(s)))
        .sum()
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let width: usize = args.get(1).map_or(64, |s| s.parse().unwrap());
    let fixed = args.get(2).is_some_and(|s| s == "true");
    let nullable = args.get(3).is_some_and(|s| s == "true");
    let binary = args.get(4).is_some_and(|s| s == "true");
    let rows: usize = args.get(5).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(6).map_or(256, |s| s.parse().unwrap());
    let warmups: usize = args.get(7).map_or(16, |s| s.parse().unwrap());
    let runs: usize = args.get(8).map_or(9, |s| s.parse().unwrap());
    let previous = std::env::var("SF_BINARY_STARTS_PREVIOUS").is_ok();
    let cp = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let lib = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap()
    );
    let previous_flag = format!("-Dsf.binary.starts.previous={previous}");
    let options = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&cp)
        .option(&lib)
        .option(&previous_flag)
        .option("-Xms256m")
        .option("-Xmx512m")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/BinaryStartsWithBenchmarkFunctions";
    let encoded = env
        .call_static_method(
            class,
            "register",
            "(IZ)[J",
            &[JValue::Int(width as i32), JValue::Bool(fixed.into())],
        )
        .unwrap()
        .l()
        .unwrap();
    let encoded = jni::objects::JLongArray::from(encoded);
    let mut program = vec![0; env.get_array_length(&encoded).unwrap() as usize];
    env.get_long_array_region(encoded, 0, &mut program).unwrap();
    env.call_static_method(
        class,
        "prepare",
        "(IZZZI)V",
        &[
            JValue::Int(width as i32),
            JValue::Bool(fixed.into()),
            JValue::Bool(nullable.into()),
            JValue::Bool(binary.into()),
            JValue::Int(rows as i32),
        ],
    )
    .unwrap();
    let mut values = Vec::with_capacity(rows);
    let mut prefixes = Vec::with_capacity(rows);
    for i in 0..rows {
        let value: Vec<u8> = (0..width).map(|j| (i * 131 + j * 127) as u8).collect();
        let length = if fixed {
            4
        } else if i % 5 == 0 {
            0
        } else if i % 7 == 0 {
            width + 1
        } else {
            width.min(4)
        };
        let mut prefix = value[..length.min(width)].to_vec();
        prefix.resize(length, 0);
        if i % 3 == 0 && !prefix.is_empty() {
            prefix[0] ^= 1;
        }
        values.push(if nullable && i % 7 == 0 {
            None
        } else {
            Some(value)
        });
        prefixes.push(if nullable && i % 11 == 0 {
            None
        } else {
            Some(prefix)
        });
    }
    let mut values: ArrayRef = Arc::new(BinaryArray::from_iter(values));
    let mut prefixes: ArrayRef = Arc::new(BinaryArray::from_iter(prefixes));
    if fixed {
        values = arrow::compute::cast(
            &values,
            &arrow::datatypes::DataType::FixedSizeBinary(width as i32),
        )
        .unwrap();
        prefixes = arrow::compute::cast(&prefixes, &arrow::datatypes::DataType::FixedSizeBinary(4))
            .unwrap();
    }
    let batch = RecordBatch::try_from_iter(vec![("b", values), ("p", prefixes)]).unwrap();
    let mut calc = CalcProgram::new(
        vec![program[0], 0, 0],
        vec![program[1], 0, 1],
        vec![2, 0, 0],
        program[2..].to_vec(),
        vec![],
        vec![],
    );
    let expected = checksum(&calc.run(batch.clone())) * loops as i64;
    for trial in 0..warmups + runs {
        for engine in (0..2).map(|i| (i + trial) % 2) {
            let name = if engine == 0 {
                "flink"
            } else if previous {
                "previous"
            } else {
                "native"
            };
            let (nanos, digest) = if engine == 0 {
                let pair = env
                    .call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                    .unwrap()
                    .l()
                    .unwrap();
                let mut values = [0; 2];
                env.get_long_array_region(jni::objects::JLongArray::from(pair), 0, &mut values)
                    .unwrap();
                (values[0] as u128, values[1])
            } else {
                let start = Instant::now();
                let mut sum = 0;
                for _ in 0..loops {
                    sum += checksum(&black_box(calc.run(black_box(batch.clone()))));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("binary_starts,{width},{fixed},{nullable},{binary},{rows},{},{trial},{},{name},{nanos},{digest}",
                rows*loops,trial<warmups);
        }
    }
    env.call_static_method(class, "close", "()V", &[]).unwrap();
}
