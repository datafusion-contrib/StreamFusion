//! Prepared production decimal casts with Rust-side timers and released Flink RowData controls.
use arrow::array::{Array, ArrayRef, Decimal128Array, StringArray};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

fn checksum(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.map_or(-1, |s| s.len() as i64))
        .sum()
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|s| s == "--oracle") {
        oracle(&args[2]);
        return;
    }
    let precision: u8 = args.get(1).map_or(18, |s| s.parse().unwrap());
    let scale: i8 = args.get(2).map_or(3, |s| s.parse().unwrap());
    let nullable = args.get(3).is_some_and(|s| s == "true");
    let binary = args.get(4).is_some_and(|s| s == "true");
    let rows: usize = args.get(5).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(6).map_or(128, |s| s.parse().unwrap());
    let warmups: usize = args.get(7).map_or(12, |s| s.parse().unwrap());
    let runs: usize = args.get(8).map_or(9, |s| s.parse().unwrap());
    let previous = std::env::var("SF_DECIMAL_STRING_PREVIOUS").is_ok();
    let try_cast = std::env::var("SF_DECIMAL_STRING_TRY_CAST").is_ok();
    let fallback = previous && try_cast;
    let cp = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let lib = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap()
    );
    let previous_flag = format!("-Dsf.decimal.string.previous={previous}");
    let try_flag = format!("-Dsf.decimal.string.tryCast={try_cast}");
    let options = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&cp)
        .option(&lib)
        .option(&previous_flag)
        .option(&try_flag)
        .option("-Xms256m")
        .option("-Xmx512m")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/DecimalStringBenchmarkFunctions";
    let encoded = env
        .call_static_method(
            class,
            "register",
            "(II)[J",
            &[JValue::Int(precision as i32), JValue::Int(scale as i32)],
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
        "(IIZZI)V",
        &[
            JValue::Int(precision as i32),
            JValue::Int(scale as i32),
            JValue::Bool(nullable.into()),
            JValue::Bool(binary.into()),
            JValue::Int(rows as i32),
        ],
    )
    .unwrap();
    let limit = 10_i128.pow(precision as u32);
    let values = Decimal128Array::from_iter((0..rows).map(|i| {
        if nullable && i % 7 == 0 {
            return None;
        }
        let mut value = if i % 17 == 0 {
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
        if i % 13 == 0 {
            value = 0;
        }
        Some(if i % 2 == 0 { -value } else { value })
    }))
    .with_precision_and_scale(precision, scale)
    .unwrap();
    let batch = RecordBatch::try_from_iter(vec![("v", Arc::new(values) as ArrayRef)]).unwrap();
    let mut calc = (!fallback).then(|| {
        CalcProgram::new(
            vec![program[0], 0],
            vec![program[1], 0],
            vec![1, 0],
            program[2..].to_vec(),
            vec![],
            vec![],
        )
    });
    let expected = if let Some(calc) = &mut calc {
        checksum(&calc.run(batch.clone())) * loops as i64
    } else {
        let result = env
            .call_static_method(class, "measure", "(I)[J", &[JValue::Int(1)])
            .unwrap()
            .l()
            .unwrap();
        let mut pair = [0; 2];
        env.get_long_array_region(jni::objects::JLongArray::from(result), 0, &mut pair)
            .unwrap();
        pair[1] * loops as i64
    };
    for trial in 0..warmups + runs {
        for engine in (0..2).map(|i| (i + trial) % 2) {
            let name = if engine == 0 {
                "flink"
            } else if previous {
                "previous"
            } else {
                "native"
            };
            let (nanos, digest) = if engine == 0 || fallback {
                let result = env
                    .call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                    .unwrap()
                    .l()
                    .unwrap();
                let mut pair = [0; 2];
                env.get_long_array_region(jni::objects::JLongArray::from(result), 0, &mut pair)
                    .unwrap();
                (pair[0] as u128, pair[1])
            } else {
                let start = Instant::now();
                let mut sum = 0;
                for _ in 0..loops {
                    sum += checksum(&black_box(
                        calc.as_mut().unwrap().run(black_box(batch.clone())),
                    ));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("decimal_string,{precision},{scale},{nullable},{binary},{rows},{},{trial},{},{name},{nanos},{digest}",
                rows * loops, trial < warmups);
        }
    }
    env.call_static_method(class, "close", "()V", &[]).unwrap();
}

fn oracle(path: &str) {
    let mut groups = std::collections::BTreeMap::<(u8, i8), Vec<(i128, String)>>::new();
    for line in std::fs::read_to_string(path).unwrap().lines() {
        let parts: Vec<_> = line.split(',').collect();
        groups
            .entry((parts[0].parse().unwrap(), parts[1].parse().unwrap()))
            .or_default()
            .push((parts[2].parse().unwrap(), parts[3].into()));
    }
    let mut count = 0;
    for ((precision, scale), rows) in groups {
        let values = Decimal128Array::from_iter_values(rows.iter().map(|r| r.0))
            .with_precision_and_scale(precision, scale)
            .unwrap();
        let batch = RecordBatch::try_from_iter(vec![("v", Arc::new(values) as ArrayRef)]).unwrap();
        let mut calc =
            CalcProgram::new(vec![11, 0], vec![6, 0], vec![1, 0], vec![], vec![], vec![]);
        let output = calc.run(batch);
        let actual = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (row, (value, expected)) in rows.iter().enumerate() {
            assert_eq!(
                actual.value(row),
                expected,
                "DECIMAL({precision},{scale}): {value}"
            );
            count += 1;
        }
    }
    println!("decimal_string oracle: {count} values matched");
}
