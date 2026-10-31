//! Prepared binary-to-character casts, consuming the first UTF-16 unit on both engines.
use arrow::array::{ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

fn program() -> CalcProgram {
    CalcProgram::new(vec![6, 0], vec![237, 0], vec![1, 0], vec![], vec![], vec![])
}

fn checksum(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| {
            s.map_or(-1, |s| {
                s.encode_utf16()
                    .next()
                    .map_or(0, |unit| i64::from(unit) + 1)
            })
        })
        .sum()
}

fn bytes(width: usize, mode: usize, row: usize) -> Vec<u8> {
    let mut bytes: Vec<_> = (0..width).map(|j| b'a' + ((row + j) % 26) as u8).collect();
    if mode == 1 {
        for unit in bytes.chunks_exact_mut(8) {
            unit.copy_from_slice(&[0xf0, 0x9f, 0x98, 0x80, 0xe4, 0xb8, 0xad, 97]);
        }
    } else if mode == 2 || mode == 3 && row % 7 == 0 {
        bytes[width - 3..].copy_from_slice(&[0xed, 0xa0, 0x80]);
    } else if mode == 4 {
        for (j, b) in bytes.iter_mut().enumerate() {
            *b = (row * 131 + j * 127) as u8;
        }
    }
    bytes
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|s| s == "--oracle") {
        oracle(&args[2]);
        return;
    }
    let width: usize = args[1].parse().unwrap();
    let mode: usize = args[2].parse().unwrap();
    let fixed = args[3] == "true";
    let nullable = args[4] == "true";
    let binary = args[5] == "true";
    let rows: usize = args[6].parse().unwrap();
    let loops: usize = args[7].parse().unwrap();
    let warmups: usize = args[8].parse().unwrap();
    let runs: usize = args[9].parse().unwrap();
    let previous = std::env::var("SF_BINARY_CHARACTER_PREVIOUS").is_ok();
    let cp = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let lib = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap()
    );
    let previous_flag = format!("-Dsf.binary.character.previous={previous}");
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
    let class = "tech/streamfusion/planner/BinaryCharacterBenchmarkFunctions";
    env.call_static_method(
        class,
        "register",
        "(IZ)V",
        &[JValue::Int(width as i32), JValue::Bool(fixed.into())],
    )
    .unwrap();
    env.call_static_method(
        class,
        "prepare",
        "(IIZZZI)V",
        &[
            JValue::Int(width as i32),
            JValue::Int(mode as i32),
            JValue::Bool(fixed.into()),
            JValue::Bool(nullable.into()),
            JValue::Bool(binary.into()),
            JValue::Int(rows as i32),
        ],
    )
    .unwrap();
    let values: ArrayRef = Arc::new(BinaryArray::from_iter((0..rows).map(|i| {
        if nullable && i % 7 == 0 {
            None
        } else {
            Some(bytes(width, mode, i))
        }
    })));
    let values = if fixed {
        arrow::compute::cast(&values, &DataType::FixedSizeBinary(width as i32)).unwrap()
    } else {
        values
    };
    let batch = RecordBatch::try_from_iter(vec![("b", values)]).unwrap();
    let mut calc = program();
    let expected = checksum(&calc.run(batch.clone())) * loops as i64;
    for trial in 0..warmups + runs {
        for engine in
            (0..if previous { 1 } else { 2 }).map(|i| (i + if previous { 0 } else { trial }) % 2)
        {
            let name = if previous {
                "previous"
            } else if engine == 0 {
                "flink"
            } else {
                "native"
            };
            let (nanos, digest) = if engine == 0 {
                let pair = env
                    .call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                    .unwrap()
                    .l()
                    .unwrap();
                let mut pair_values = [0; 2];
                env.get_long_array_region(
                    jni::objects::JLongArray::from(pair),
                    0,
                    &mut pair_values,
                )
                .unwrap();
                (pair_values[0] as u128, pair_values[1])
            } else {
                let start = Instant::now();
                let mut sum = 0;
                for _ in 0..loops {
                    sum += checksum(&black_box(calc.run(black_box(batch.clone()))));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("binary_character,{width},{mode},{fixed},{nullable},{binary},{rows},{},{trial},{},{name},{nanos},{digest}",
                rows * loops, trial < warmups);
        }
    }
    env.call_static_method(class, "close", "()V", &[]).unwrap();
}

fn unhex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
            digit(pair[0]) * 16 + digit(pair[1])
        })
        .collect()
}

fn oracle(path: &str) {
    let records: Vec<_> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter_map(|s| s.split_once(',').map(|(a, b)| (unhex(a), unhex(b))))
        .collect();
    let mut calc = program();
    for chunk in records.chunks(1024) {
        let batch = RecordBatch::try_from_iter(vec![(
            "b",
            Arc::new(BinaryArray::from_iter_values(
                chunk.iter().map(|(bytes, _)| bytes.as_slice()),
            )) as ArrayRef,
        )])
        .unwrap();
        let output = calc.run(batch);
        let strings = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (i, (bytes, expected)) in chunk.iter().enumerate() {
            assert_eq!(strings.value(i).as_bytes(), expected, "bytes: {bytes:?}");
        }
    }
    println!("binary_character oracle: {} values matched", records.len());
}
