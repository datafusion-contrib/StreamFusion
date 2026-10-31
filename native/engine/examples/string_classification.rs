//! Released RowData expressions versus the production Arrow Calc, timed in their own runtimes.
use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

fn value(corpus: &str, width: usize, row: usize) -> String {
    let unit = if matches!(corpus, "bmp" | "unicode") {
        "\u{4e2d}\u{6587}\u{e9}\u{3a9}"
    } else {
        "abcXYZ"
    };
    let body: String = unit.chars().cycle().take(width).collect();
    if corpus == "supplementary" {
        return format!("a\u{10400}{body}");
    }
    if matches!(corpus, "mixed" | "unicode") {
        if row % 17 == 0 {
            return String::new();
        }
        if row % 3 == 0 {
            return format!(
                "{body}{}",
                if corpus == "unicode" { "\u{301}" } else { "7" }
            );
        }
    }
    format!("{}{body}", char::from(b'a' + (row % 26) as u8))
}

fn checksum(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .values()
        .count_set_bits() as i64
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let corpus = args.get(1).map_or("mixed", String::as_str);
    let width: usize = args.get(2).map_or(16, |s| s.parse().unwrap());
    let nullable = args.get(3).is_some_and(|s| s == "true");
    let binary = args.get(4).is_some_and(|s| s == "true");
    let rows: usize = args.get(5).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(6).map_or(128, |s| s.parse().unwrap());
    let warmups: usize = args.get(7).map_or(16, |s| s.parse().unwrap());
    let runs: usize = args.get(8).map_or(9, |s| s.parse().unwrap());
    let previous = std::env::var("SF_CLASSIFICATION_PREVIOUS").is_ok();
    let cp = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let lib = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap()
    );
    let flag = format!("-Dsf.classification.previous={previous}");
    let options = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&cp)
        .option(&lib)
        .option(&flag)
        .option("-Xms256m")
        .option("-Xmx512m")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/StringClassificationBenchmarkFunctions";
    let encoded = env
        .call_static_method(class, "register", "()[J", &[])
        .unwrap()
        .l()
        .unwrap();
    let encoded = jni::objects::JLongArray::from(encoded);
    let mut program = vec![0; env.get_array_length(&encoded).unwrap() as usize];
    env.get_long_array_region(&encoded, 0, &mut program)
        .unwrap();
    let corpus_java = env.new_string(corpus).unwrap();
    env.call_static_method(
        class,
        "prepare",
        "(Ljava/lang/String;IZZI)V",
        &[
            JValue::Object(&corpus_java),
            JValue::Int(width as i32),
            JValue::Bool(nullable.into()),
            JValue::Bool(binary.into()),
            JValue::Int(rows as i32),
        ],
    )
    .unwrap();
    let strings = StringArray::from_iter(
        (0..rows).map(|row| (!(nullable && row % 7 == 0)).then(|| value(corpus, width, row))),
    );
    let batch = RecordBatch::try_from_iter(vec![("v", Arc::new(strings) as ArrayRef)]).unwrap();
    let mut calc = CalcProgram::new(
        vec![program[0], 0],
        vec![program[1], 0],
        vec![1, 0],
        program[2..].to_vec(),
        vec![],
        vec![],
    );
    let expected = checksum(&calc.run(batch.clone())) * loops as i64;
    for trial in 0..warmups + runs {
        for engine in (0..2).map(|turn| (turn + trial) % 2) {
            let name = if engine == 0 {
                "flink"
            } else if previous {
                "previous"
            } else {
                "native"
            };
            let (nanos, digest) = if engine == 0 {
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
                    sum += checksum(&black_box(calc.run(black_box(batch.clone()))));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("classification,{corpus},{width},{nullable},{binary},{rows},{},{trial},{},{name},{nanos},{digest}",rows*loops,trial<warmups);
        }
    }
    env.call_static_method(class, "close", "()V", &[]).unwrap();
}
