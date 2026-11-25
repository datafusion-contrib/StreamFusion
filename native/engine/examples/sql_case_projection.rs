//! Prepared SQL UPPER and LOWER, timed on the stock, callback and native execution paths.
use arrow::array::{Array, ArrayRef, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::{JLongArray, JValue};
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;
use streamfusion_bridge::string_cache::{string_cache_array, string_cache_field};

fn values(output: &RecordBatch) -> &ArrayRef {
    let value = output.column(0);
    if let Some(represented) = value.as_any().downcast_ref::<StructArray>() {
        represented.column(0)
    } else {
        value
    }
}

fn checksum(output: &RecordBatch) -> i64 {
    values(output)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.map_or(-1, |text| text.len() as i64))
        .sum()
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let upper = args.get(1).is_none_or(|s| s == "upper");
    let unicode = args.get(2).is_some_and(|s| s == "unicode");
    let mixed = args.get(3).is_some_and(|s| s == "mixed");
    let width = args.get(4).map_or(64, |s| s.parse::<usize>().unwrap());
    let batch_rows = args.get(5).map_or(1024, |s| s.parse::<usize>().unwrap());
    let loops = args.get(6).map_or(64, |s| s.parse::<usize>().unwrap());
    let warmups = args.get(7).map_or(8, |s| s.parse::<usize>().unwrap());
    let runs = args.get(8).map_or(9, |s| s.parse::<usize>().unwrap());
    let classpath = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let library = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap_or_default()
    );
    let options = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&classpath)
        .option(&library)
        .option("-Xms256m")
        .option("-Xmx512m")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/CaseProjectionBenchmarkFunctions";
    let id = env
        .call_static_method(
            class,
            "prepare",
            "(ZZZII)I",
            &[
                JValue::Bool(upper.into()),
                JValue::Bool(unicode.into()),
                JValue::Bool(mixed.into()),
                JValue::Int(width as i32),
                JValue::Int(batch_rows as i32),
            ],
        )
        .unwrap()
        .i()
        .unwrap();
    let empty = env.new_long_array(0).unwrap();
    let words = env
        .call_static_method(
            "tech/streamfusion/operator/JdkCharacterTable",
            "appendCaseMappings",
            "([JZ)[J",
            &[JValue::Object(&empty), JValue::Bool(upper.into())],
        )
        .unwrap()
        .l()
        .unwrap();
    let words = JLongArray::from(words);
    let mut mappings = vec![0; env.get_array_length(&words).unwrap() as usize];
    env.get_long_array_region(&words, 0, &mut mappings).unwrap();

    let unit = if unicode { "aßIΣ中🙂" } else { "aBI!x" };
    let body = unit.repeat((width / unit.len()).max(1));
    let strings: Vec<_> = (0..batch_rows)
        .map(|row| (row % 11 != 0).then(|| format!("{body}{}", row % 16)))
        .collect();
    let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(strings.clone()))];
    let mut fields = vec![Field::new("s", DataType::Utf8, true)];
    if mixed {
        let caches: Vec<Option<Vec<u16>>> = strings
            .iter()
            .enumerate()
            .map(|(row, value)| {
                if row % 2 == 0 {
                    value.as_ref().map(|v| v.encode_utf16().collect())
                } else {
                    None
                }
            })
            .collect();
        let caches = string_cache_array(caches.iter().map(|value| value.as_deref()));
        fields.push(string_cache_field(0, caches.data_type().clone()));
        columns.push(caches);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut previous = CalcProgram::new(
        vec![44, 0],
        vec![0, 0],
        vec![1, 0],
        vec![i64::from(id), 0, 0],
        vec![],
        vec![],
    );
    let mut candidate = CalcProgram::new(
        vec![51, 0],
        vec![0, 0],
        vec![1, 0],
        mappings,
        vec![],
        vec![],
    );
    let reference = previous.run(batch.clone());
    assert_eq!(values(&reference), values(&candidate.run(batch.clone())));
    let expected = checksum(&reference) * loops as i64;
    for trial in 0..warmups + runs {
        for turn in 0..3 {
            let engine = (trial + turn) % 3;
            let (nanos, digest) = if engine == 0 {
                let result = env
                    .call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                    .unwrap()
                    .l()
                    .unwrap();
                let mut values = [0; 2];
                env.get_long_array_region(JLongArray::from(result), 0, &mut values)
                    .unwrap();
                (values[0] as u128, values[1])
            } else {
                let calc = if engine == 1 {
                    &mut previous
                } else {
                    &mut candidate
                };
                let start = Instant::now();
                let mut digest = 0;
                for _ in 0..loops {
                    digest += checksum(&black_box(calc.run(black_box(batch.clone()))));
                }
                (start.elapsed().as_nanos(), black_box(digest))
            };
            assert_eq!(digest, expected);
            println!("case,{upper},{unicode},{mixed},{width},{batch_rows},{},{trial},{},{},{nanos},{digest}",
                loops * batch_rows, trial < warmups, ["flink", "previous", "native"][engine]);
        }
    }
    env.call_static_method(
        "tech/streamfusion/operator/NativeUdf",
        "unregister",
        "(I)V",
        &[JValue::Int(id)],
    )
    .unwrap();
}
