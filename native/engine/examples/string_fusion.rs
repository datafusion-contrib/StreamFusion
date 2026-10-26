//! Production Calc versus its retained JVM evaluator and released generated Flink expressions.
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;
use streamfusion_bridge::string_cache::{string_cache_array, string_cache_field};

fn plan(id: i32, operation: i64, certificate: i64, target: &str) -> CalcProgram {
    let replace = operation >= 26;
    let overlay = (operation - 20) % 3 == 2;
    let mut kinds = if replace {
        vec![44, 0, 0, 0, 3, 3]
    } else {
        vec![44, 0, 0, 0, 3]
    };
    let mut payload = if replace {
        vec![
            0,
            0,
            if overlay { 2 } else { 1 },
            if overlay { 1 } else { 2 },
            0,
            1,
        ]
    } else {
        vec![
            0,
            0,
            if overlay { 2 } else { 1 },
            if overlay { 1 } else { 2 },
            0,
        ]
    };
    let mut children = if replace {
        vec![5, 0, 0, 0, 0, 0]
    } else {
        vec![4, 0, 0, 0, 0]
    };
    if certificate == 0 {
        kinds.truncate(4);
        payload.truncate(4);
        children.truncate(4);
        children[0] = 3;
    }
    CalcProgram::new(
        kinds,
        payload,
        children,
        vec![i64::from(id), if replace { 0 } else { 4 }, certificate],
        vec![],
        vec![
            Some(if replace { "?" } else { target }.into()),
            Some("Z".into()),
        ],
    )
}

fn checksum(batch: &RecordBatch) -> i64 {
    if let Some(values) = batch.column(0).as_any().downcast_ref::<StringArray>() {
        values
            .iter()
            .map(|value| value.map_or(-1, |s| s.len() as i64))
            .sum()
    } else {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .map(|value| value.map_or(-1, i64::from))
            .sum()
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let operation: i64 = args.get(1).map_or(20, |s| s.parse().unwrap());
    let corpus = args.get(2).map_or("ascii", String::as_str);
    let width: usize = args.get(3).map_or(264, |s| s.parse().unwrap());
    let cached = args.get(4).is_some_and(|s| s == "true");
    let batch_rows: usize = args.get(5).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(6).map_or(256, |s| s.parse().unwrap());
    let warmups: usize = args.get(7).map_or(5, |s| s.parse().unwrap());
    let runs: usize = args.get(8).map_or(9, |s| s.parse().unwrap());
    let engine_filter = args.get(9).map_or("all", String::as_str);
    let growth: usize = args.get(10).map_or(0, |s| s.parse().unwrap());
    let matching = args.get(11).is_some_and(|s| s == "true");
    assert!((20..=28).contains(&operation));
    assert!(growth == 0 || matches!(operation, 20 | 21 | 23 | 24));
    let classpath = format!(
        "-Djava.class.path={}",
        std::env::var("SF_NATIVE_BENCH_CLASSPATH").unwrap()
    );
    let library = format!(
        "-Djava.library.path={}",
        std::env::var("SF_NATIVE_LIB_DIR").unwrap_or_default()
    );
    let arguments = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&classpath)
        .option(&library)
        .option("-Xms256m")
        .option("-Xmx512m")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(arguments).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let unit = match corpus {
        "cjk" => "\u{4e2d}\u{6587}?",
        "emoji" => "\u{1f600}?",
        _ => "ab?c",
    };
    let repeats = (width / unit.len()).max(1);
    let base = format!("a?{}", unit.repeat(repeats));
    let pad = match (growth > 0, corpus) {
        (true, "cjk") => "\u{4e2d}",
        (true, "emoji") => "\u{1f600}",
        _ => "X",
    };
    let pad_units: Vec<_> = pad.encode_utf16().collect();
    assert!(!matching || growth.is_multiple_of(pad_units.len()));
    let overlay = (operation - 20) % 3 == 2;
    let prefix = 2 + unit.encode_utf16().count() * (repeats / 2);
    let target = if growth > 0 {
        let source = format!("{base}|0");
        let padding: String = char::decode_utf16(pad_units.iter().copied().cycle().take(growth))
            .map(|unit| unit.unwrap_or('?'))
            .collect();
        if (operation - 20) % 3 == 0 {
            format!("{padding}{source}")
        } else {
            format!("{source}{padding}")
        }
    } else if overlay {
        format!("X{}|0", &base[1..])
    } else {
        String::from_utf16(&base.encode_utf16().take(prefix).collect::<Vec<_>>()).unwrap()
    };
    let sources: Vec<_> = (0..batch_rows)
        .map(|i| (i % 7 != 0).then(|| format!("{base}|{}", if matching { 0 } else { i % 16 })))
        .collect();
    let units: Vec<_> = sources
        .iter()
        .map(|s| s.as_ref().map(|s| s.encode_utf16().collect::<Vec<_>>()))
        .collect();
    let numbers: Vec<_> = (0..batch_rows)
        .map(|i| {
            if growth > 0 {
                units[i].as_ref().map_or(0, Vec::len) + growth + if matching { 0 } else { i % 3 }
            } else if overlay {
                1 + i % 3
            } else {
                prefix + i % 3
            }
        })
        .collect();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sources)),
        if overlay {
            Arc::new(Int64Array::from_iter_values(
                numbers.iter().map(|n| *n as i64),
            ))
        } else {
            Arc::new(Int32Array::from_iter_values(
                numbers.iter().map(|n| *n as i32),
            ))
        },
        Arc::new(StringArray::from(vec![pad; batch_rows])),
    ];
    let mut fields = vec![
        Field::new("s", DataType::Utf8, true),
        Field::new(
            "n",
            if overlay {
                DataType::Int64
            } else {
                DataType::Int32
            },
            false,
        ),
        Field::new("p", DataType::Utf8, false),
    ];
    if cached {
        for (index, cache) in [
            (0, string_cache_array(units.iter().map(|v| v.as_deref()))),
            (
                2,
                string_cache_array((0..batch_rows).map(|_| Some(pad_units.as_slice()))),
            ),
        ] {
            fields.push(string_cache_field(index, cache.data_type().clone()));
            columns.push(cache);
        }
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let class = "tech/streamfusion/planner/StringFusionBenchmarkFunctions";
    let mut env = vm.attach_current_thread().unwrap();
    let target_java = env.new_string(&target).unwrap();
    let corpus_java = env.new_string(corpus).unwrap();
    let id = env.call_static_method(
        class,
        "register",
        "(ILjava/lang/String;)I",
        &[JValue::Int(operation as i32), JValue::Object(&target_java)],
    );
    if env.exception_check().unwrap() {
        env.exception_describe().unwrap();
    }
    let id = id.unwrap().i().unwrap();
    env.call_static_method(
        class,
        "prepare",
        "(ILjava/lang/String;Ljava/lang/String;IZIIZ)V",
        &[
            JValue::Int(operation as i32),
            JValue::Object(&target_java),
            JValue::Object(&corpus_java),
            JValue::Int(width as i32),
            JValue::Bool(cached.into()),
            JValue::Int(batch_rows as i32),
            JValue::Int(growth as i32),
            JValue::Bool(matching.into()),
        ],
    )
    .unwrap();
    // Baseline mode is linked against the preceding production library by the comparison driver.
    let mut prior = plan(
        id,
        operation,
        if engine_filter == "baseline" {
            operation
        } else {
            0
        },
        &target,
    );
    let admitted = !(operation >= 26 && cached && corpus != "ascii");
    let mut candidate = plan(
        if admitted { -1 } else { id },
        operation,
        operation,
        &target,
    );
    let reference = prior.run(batch.clone());
    if matches!(engine_filter, "all" | "native") {
        assert_eq!(reference.column(0), candidate.run(batch.clone()).column(0));
    }
    for trial in 0..warmups + runs {
        for turn in 0..3 {
            let engine = (trial + turn) % 3;
            if engine_filter != "all"
                && engine_filter != ["flink", "previous", "native"][engine]
                && !(engine_filter == "baseline" && engine == 1)
            {
                continue;
            }
            let (nanos, digest) = if engine == 0 {
                let result = env
                    .call_static_method(class, "measure", "(I)[J", &[JValue::Int(loops as i32)])
                    .unwrap()
                    .l()
                    .unwrap();
                let mut values = [0; 2];
                env.get_long_array_region(jni::objects::JLongArray::from(result), 0, &mut values)
                    .unwrap();
                (values[0] as u128, values[1])
            } else {
                let calc = if engine == 1 {
                    &mut prior
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
            assert_eq!(digest, checksum(&reference) * loops as i64);
            println!("fusion,{operation},{corpus},{width},{cached},{batch_rows},{},{trial},{},{},{nanos},{digest},{growth},{matching}",
                loops * batch_rows, trial < warmups, ["flink", "previous", if admitted { "native" } else { "retained" }][engine]);
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
