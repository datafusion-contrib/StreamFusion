//! Compare released Flink with the existing Utf8 and lossless Binary Calc paths.
use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

fn checksum(batch: &RecordBatch) -> i64 {
    let column = batch.column(0);
    if let Some(values) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return values.iter().map(|value| value.map_or(-1, i64::from)).sum();
    }
    let bytes;
    let values = if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        values
    } else {
        let values = column.as_any().downcast_ref::<StringArray>().unwrap();
        bytes = BinaryArray::new(
            values.offsets().clone(),
            values.values().clone(),
            values.nulls().cloned(),
        );
        &bytes
    };
    values
        .iter()
        .map(|value| {
            value.map_or(-1, |v| {
                (v.len()
                    + v.first().copied().unwrap_or(0) as usize
                    + v.last().copied().unwrap_or(0) as usize) as i64
            })
        })
        .sum()
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let width: usize = args.get(1).map_or(16, |s| s.parse().unwrap());
    let unicode = args.get(2).is_some_and(|s| s == "true");
    let nullable = args.get(3).is_some_and(|s| s == "true");
    let invalid_every: usize = args.get(4).map_or(0, |s| s.parse().unwrap());
    let function = args.get(5).map_or("concat", String::as_str);
    let rows = 4096;
    let loops = 256;
    let warmups = 12;
    let runs = 9;
    let cp = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let options = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&cp)
        .option("-Xmx1g")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/ByteStringConcatBenchmarkFunctions";
    let java_function = env.new_string(function).unwrap();
    env.call_static_method(
        class,
        "prepare",
        "(IZZIILjava/lang/String;)V",
        &[
            JValue::Int(width as i32),
            JValue::Bool(unicode.into()),
            JValue::Bool(nullable.into()),
            JValue::Int(invalid_every as i32),
            JValue::Int(rows as i32),
            JValue::Object(&java_function),
        ],
    )
    .unwrap_or_else(|error| {
        let _ = env.exception_describe();
        panic!("{error}")
    });
    eprintln!("Prepared released Flink rows");
    let values: Vec<_> = (0..rows)
        .map(|row| {
            (!nullable || row % 7 != 0).then(|| {
                let mut value = if unicode { "\u{4e2d}" } else { "a" }
                    .repeat(width)
                    .into_bytes();
                if invalid_every > 0 && row % invalid_every == 0 && !value.is_empty() {
                    value[0] = 255;
                }
                value
            })
        })
        .collect();
    let binary = BinaryArray::from_iter(values.iter().map(|value| value.as_deref()));
    let mut inputs: Vec<ArrayRef> = vec![Arc::new(binary.clone())];
    if invalid_every == 0 {
        inputs.push(Arc::new(StringArray::try_from_binary(binary).unwrap()));
        if function.starts_with("sha2_") {
            inputs.push(inputs[1].clone());
        }
    }
    let mut calcs: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(index, _)| {
            if index == 2 {
                let bits: i64 = function.strip_prefix("sha2_").unwrap().parse().unwrap();
                return CalcProgram::new(
                    vec![44, 0, 7],
                    vec![0, 0, 3],
                    vec![2, 0, 0],
                    vec![-1, 0, 12, bits],
                    vec![],
                    vec![],
                );
            }
            let (kinds, payload, children, strings) = match function {
                "concat" => (
                    vec![6, 0, 3],
                    vec![93, 0, 0],
                    vec![2, 0, 0],
                    vec![Some("!".into())],
                ),
                "concat_ws" => (
                    vec![6, 3, 0, 3],
                    vec![94, 0, 0, 1],
                    vec![3, 0, 0, 0],
                    vec![Some(",".into()), Some("!".into())],
                ),
                "concat_ws_separator" => (
                    vec![6, 0, 3, 3],
                    vec![94, 0, 0, 1],
                    vec![3, 0, 0, 0],
                    vec![Some("x".into()), Some("!".into())],
                ),
                "md5" => (vec![6, 0], vec![95, 0], vec![1, 0], vec![]),
                "sha1" => (vec![6, 0], vec![143, 0], vec![1, 0], vec![]),
                "sha224" | "sha2_224" => (vec![6, 0], vec![96, 0], vec![1, 0], vec![]),
                "sha256" | "sha2_256" => (vec![6, 0], vec![97, 0], vec![1, 0], vec![]),
                "sha384" | "sha2_384" => (vec![6, 0], vec![98, 0], vec![1, 0], vec![]),
                "sha512" | "sha2_512" => (vec![6, 0], vec![99, 0], vec![1, 0], vec![]),
                "to_base64" => (vec![6, 0], vec![107, 0], vec![1, 0], vec![]),
                "ascii" => (vec![6, 0], vec![67, 0], vec![1, 0], vec![]),
                _ => panic!("Unknown byte-string benchmark: {function}"),
            };
            CalcProgram::new(kinds, payload, children, vec![], vec![], strings)
        })
        .collect();
    let batches: Vec<_> = inputs
        .into_iter()
        .map(|input| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "s",
                    input.data_type().clone(),
                    true,
                )])),
                vec![input],
            )
            .unwrap()
        })
        .collect();
    let expected = checksum(&calcs[0].run(batches[0].clone())) * loops;
    eprintln!("Compiled native Calc");
    for (calc, batch) in calcs.iter_mut().zip(&batches) {
        assert_eq!(checksum(&calc.run(batch.clone())) * loops, expected);
    }
    let engines = calcs.len() + 1;
    for trial in 0..warmups + runs {
        for engine in (0..engines).map(|i| (i + trial) % engines) {
            let (name, nanos, digest) = if engine == 0 {
                let result = env
                    .call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                    .unwrap()
                    .l()
                    .unwrap();
                let mut pair = [0; 2];
                env.get_long_array_region(jni::objects::JLongArray::from(result), 0, &mut pair)
                    .unwrap();
                ("flink", pair[0] as u128, pair[1])
            } else {
                let calc = &mut calcs[engine - 1];
                let batch = &batches[engine - 1];
                let start = Instant::now();
                let mut digest = 0;
                for _ in 0..loops {
                    digest += checksum(&black_box(calc.run(black_box(batch.clone()))));
                }
                (
                    match engine {
                        1 => "binary",
                        2 => "utf8",
                        3 => "previous",
                        _ => unreachable!(),
                    },
                    start.elapsed().as_nanos(),
                    black_box(digest),
                )
            };
            assert_eq!(digest, expected);
            println!("byte_{function},{width},{unicode},{nullable},{invalid_every},{trial},{},{name},{nanos}", trial < warmups);
        }
    }
    env.call_static_method(class, "close", "()V", &[]).unwrap();
}
