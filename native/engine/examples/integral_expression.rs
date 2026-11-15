//! Release stock/previous/native integer-expression controls; native timers stay inside Rust.
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, StructArray,
};
use arrow::array::{Int32Builder, ListBuilder, StringBuilder};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use jni::objects::JValue;
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::{
    timestamp_array, timestamp_type, TimestampColumn, TimestampValue,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(serde::Deserialize)]
struct EncodedProgram {
    kinds: Vec<i64>,
    payload: Vec<i64>,
    child_counts: Vec<i64>,
    longs: Vec<i64>,
    doubles: Vec<f64>,
    strings: Vec<Option<String>>,
    boolean_output: bool,
    #[serde(default)]
    string_output: bool,
    #[serde(default)]
    long_output: bool,
    #[serde(default)]
    integer_output: Option<String>,
    #[serde(default = "first_projection")]
    projection_roots: Vec<usize>,
    #[serde(default = "one_output")]
    output_count: usize,
}

fn first_projection() -> Vec<usize> {
    vec![0]
}
fn one_output() -> usize {
    1
}

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
        .columns()
        .iter()
        .fold(0i64, |sum, array| sum.wrapping_add(column_checksum(array)))
}

fn column_checksum(array: &ArrayRef) -> i64 {
    macro_rules! sum {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .iter()
                .fold(0i64, |sum, v| sum.wrapping_add(v.map_or(-1, i64::from)))
        };
    }
    match array.data_type() {
        DataType::Boolean => sum!(BooleanArray),
        DataType::Int8 => sum!(Int8Array),
        DataType::Int16 => sum!(Int16Array),
        DataType::Int32 => sum!(Int32Array),
        DataType::Int64 => sum!(Int64Array),
        DataType::Utf8 => string_checksum(array, array),
        DataType::Struct(fields) if fields[0].name() == "utf8" => {
            let represented = array.as_any().downcast_ref::<StructArray>().unwrap();
            string_checksum(array, represented.column(0))
        }
        ty if *ty == timestamp_type() => {
            let values = TimestampColumn::try_new(array.as_ref()).unwrap();
            (0..values.len()).fold(0i64, |sum, row| {
                sum.wrapping_add(if values.is_null(row) {
                    -1
                } else {
                    let value = values.value(row).unwrap();
                    value
                        .millis()
                        .wrapping_add(i64::from(value.nano_of_milli()))
                })
            })
        }
        other => panic!("Unsupported exact-numeric-expression output {other}"),
    }
}

fn string_checksum(logical: &ArrayRef, text: &ArrayRef) -> i64 {
    let text = text.as_any().downcast_ref::<StringArray>().unwrap();
    (0..logical.len())
        .map(|row| {
            if logical.is_null(row) {
                -1
            } else {
                let value = text.value(row);
                value.len() as i64
                    + value.as_bytes().first().map_or(0, |v| i64::from(*v))
                    + value.as_bytes().last().map_or(0, |v| i64::from(*v))
            }
        })
        .sum()
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let element = args.get(1).map_or("INT", String::as_str);
    let nullable = args.get(2).is_some_and(|s| s == "true");
    let binary = args.get(3).is_some_and(|s| s == "true");
    let rows: usize = args.get(4).map_or(1024, |s| s.parse().unwrap());
    let loops: usize = args.get(5).map_or(128, |s| s.parse().unwrap());
    let warmups: usize = args.get(6).map_or(12, |s| s.parse().unwrap());
    let runs: usize = args.get(7).map_or(9, |s| s.parse().unwrap());
    let previous = std::env::var("SF_INTEGRAL_PREVIOUS").is_ok();
    let epoch_millis = std::env::var("SF_INTEGRAL_CORPUS").as_deref() == Ok("epoch-millis");
    let epoch_seconds = std::env::var("SF_INTEGRAL_CORPUS").as_deref() == Ok("epoch-seconds");
    let expression = std::env::var("SF_INTEGRAL_EXPRESSION")
        .unwrap_or_else(|_| "ABS(v+CAST(1 AS $TYPE))".into());
    let previous_option = format!("-Dsf.integral.previous={previous}");
    let previous_native_option = format!(
        "-Dsf.integral.previousNative={}",
        std::env::var("SF_INTEGRAL_PREVIOUS_NATIVE").is_ok()
    );
    let expression_option = format!("-Dsf.integral.expression={expression}");
    let secondary = std::env::var("SF_INTEGRAL_SECONDARY_TYPE").unwrap_or_default();
    let secondary_option = format!("-Dsf.integral.secondaryType={secondary}");
    let legacy_option = format!(
        "-Dsf.integral.legacy={}",
        std::env::var("SF_INTEGRAL_LEGACY").is_ok()
    );
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
        .option(&previous_option)
        .option(&previous_native_option)
        .option(&expression_option)
        .option(&secondary_option)
        .option(&legacy_option)
        .option("-Xmx1g")
        .option("-Dstreamfusion.native.development=true")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(options).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut env = vm.attach_current_thread().unwrap();
    let class = "tech/streamfusion/planner/IntegralExpressionBenchmarkFunctions";
    let ty = env.new_string(element).unwrap();
    let program = java!(
        env,
        env.call_static_method(
            class,
            "register",
            "(Ljava/lang/String;ZZI)Ljava/lang/String;",
            &[
                JValue::Object(&ty),
                JValue::Bool(nullable.into()),
                JValue::Bool(binary.into()),
                JValue::Int(rows as i32)
            ]
        )
    )
    .l()
    .unwrap();
    let program: String = env
        .get_string(&jni::objects::JString::from(program))
        .unwrap()
        .into();
    let program: EncodedProgram = serde_json::from_str(&program).unwrap();
    let value = |element: &str, i: usize| {
        let v = (i as i64) * 7919;
        match element {
            "TINYINT" => i64::from(if i % 17 == 0 { i8::MIN } else { v as i8 }),
            "SMALLINT" => i64::from(if i % 17 == 0 { i16::MIN } else { v as i16 }),
            "INT" => {
                if i % 17 == 0 {
                    i64::from(i32::MIN)
                } else {
                    (if i % 2 == 0 { -v } else { v }) as i32 as i64
                }
            }
            "BIGINT" => {
                if epoch_seconds {
                    let epochs = [
                        i64::MIN,
                        i64::MAX,
                        i64::MIN / 1000 - 1,
                        i64::MAX / 1000 + 1,
                        -62_167_219_201,
                        -62_167_219_200,
                        -62_167_219_199,
                        253_402_300_798,
                        253_402_300_799,
                        253_402_300_800,
                        -62_135_596_800,
                        -12_219_292_800,
                        -86_401,
                        -1,
                        0,
                        1,
                        951_827_696,
                        1_706_659_200,
                        1_710_037_800,
                    ];
                    let index = i % epochs.len();
                    epochs[index]
                        + if index < 10 {
                            0
                        } else {
                            (i / epochs.len()) as i64 * 7919
                        }
                } else if epoch_millis {
                    let epochs = [
                        i64::MIN,
                        i64::MAX,
                        -62_167_219_200_001,
                        -62_167_219_200_000,
                        -62_167_219_199_999,
                        253_402_300_799_999,
                        253_402_300_800_000,
                        -62_135_596_800_000,
                        -12_219_292_800_000,
                        -86_400_001,
                        -1,
                        0,
                        1,
                        951_827_696_123,
                        1_706_659_200_000,
                        1_710_037_800_000,
                    ];
                    let index = i % epochs.len();
                    epochs[index]
                        + if index < 7 {
                            0
                        } else {
                            (i / epochs.len()) as i64 * 7919
                        }
                } else if i % 17 == 0 {
                    i64::MIN
                } else {
                    (if i % 2 == 0 { -1 } else { 1 }) * (9_007_199_254_740_993 + v)
                }
            }
            _ => panic!("Unsupported integer-expression input {element}"),
        }
    };
    let values = (0..rows).map(|i| (!nullable || i % 7 != 0).then(|| value(element, i)));
    let input: ArrayRef = match element {
        "ARRAY<INT>" | "ARRAY<VARCHAR(128)>" | "ROW<z INT,a VARCHAR(128)>" => {
            json_container_input(element, rows, nullable)
        }
        "VARCHAR(4096)" => {
            let corpus = std::env::var("SF_INTEGRAL_CORPUS").unwrap_or_default();
            let documents: Vec<String> = (0..rows).map(|row| json_text(row, &corpus)).collect();
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (!nullable || i % 7 != 0).then_some(documents[i].as_str())
            })))
        }
        "BOOLEAN" => Arc::new(BooleanArray::from_iter(
            (0..rows).map(|i| (!nullable || i % 7 != 0).then_some(i % 2 == 0)),
        )),
        "TINYINT" => Arc::new(Int8Array::from_iter(values.map(|v| v.map(|v| v as i8)))),
        "SMALLINT" => Arc::new(Int16Array::from_iter(values.map(|v| v.map(|v| v as i16)))),
        "INT" => Arc::new(Int32Array::from_iter(values.map(|v| v.map(|v| v as i32)))),
        "BIGINT" => Arc::new(Int64Array::from_iter(values)),
        "TIMESTAMP_LTZ(3)" | "TIMESTAMP_LTZ(9)" => {
            let epochs = [
                i64::MIN,
                i64::MAX,
                -62_135_596_800_000,
                -12_219_292_800_000,
                -2_208_988_800_000,
                -86_400_001,
                -1,
                0,
                1,
                951_827_696_123,
                1_706_659_200_000,
                1_710_037_800_000,
                253_402_300_799_999,
            ];
            Arc::new(timestamp_array((0..rows).map(|row| {
                (!nullable || row % 7 != 0).then(|| {
                    let index = row % epochs.len();
                    let millis = epochs[index]
                        + if index < 2 {
                            0
                        } else {
                            (row / epochs.len()) as i64 * 7919
                        };
                    let nanos = if element == "TIMESTAMP_LTZ(9)" {
                        (row * 7919 % 1_000_000) as u32
                    } else {
                        0
                    };
                    TimestampValue::new(millis, nanos).unwrap()
                })
            })))
        }
        ty if ty.starts_with("DECIMAL(") => {
            let (precision, scale) = ty[8..ty.len() - 1].split_once(',').unwrap();
            let precision: u8 = precision.parse().unwrap();
            let scale: i8 = scale.parse().unwrap();
            let maximum = 10_i128.pow(precision as u32) - 1;
            let decimal_truth =
                std::env::var("SF_INTEGRAL_CORPUS").is_ok_and(|value| value == "decimal-truth");
            let epochs = (std::env::var("SF_INTEGRAL_CORPUS").as_deref() == Ok("decimal-epoch"))
                .then(|| decimal_epoch_coefficients(precision, scale));
            let values = (0..rows).map(|row| {
                if let Some(values) = &epochs {
                    return (!nullable || row % 7 != 0).then(|| values[row % values.len()]);
                }
                (!nullable || row % 7 != 0).then(|| match row % 17 {
                    0 => maximum,
                    1 => -maximum,
                    2 if decimal_truth => 0,
                    _ if precision == 4 => {
                        (if row % 2 == 0 { -1 } else { 1 }) * (row as i128 * 7919 % 10_000)
                    }
                    _ => {
                        (if row % 2 == 0 { -1 } else { 1 })
                            * (9_007_199_254_740_993 + row as i128 * 7919)
                    }
                })
            });
            Arc::new(
                Decimal128Array::from_iter(values)
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            )
        }
        _ => panic!("Unsupported exact-numeric-expression input {element}"),
    };
    let mut fields = vec![Field::new("v", input.data_type().clone(), nullable)];
    let mut columns = vec![input];
    if !secondary.is_empty() {
        let values =
            (0..rows).map(|i| (!nullable || i % 11 != 0).then(|| value(&secondary, i + 3)));
        let input: ArrayRef = match secondary.as_str() {
            "INT" => Arc::new(Int32Array::from_iter(values.map(|v| v.map(|v| v as i32)))),
            "BIGINT" => Arc::new(Int64Array::from_iter(values)),
            "VARCHAR(4096)" => {
                let corpus = std::env::var("SF_INTEGRAL_CORPUS").unwrap_or_default();
                Arc::new(StringArray::from_iter((0..rows).map(|row| {
                    (!nullable || row % 11 != 0).then(|| json_text(row + 3, &corpus))
                })))
            }
            _ => panic!("Unsupported secondary exact-numeric input {secondary}"),
        };
        fields.push(Field::new("n", input.data_type().clone(), nullable));
        columns.push(input);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let output_names = (0..program.output_count)
        .map(|index| format!("result{index}"))
        .collect();
    let mut native = CalcProgram::new(
        program.kinds,
        program.payload,
        program.child_counts,
        program.longs,
        program.doubles,
        program.strings,
    )
    .with_projections(program.projection_roots, output_names);
    let actual = native.run(batch.clone());
    assert_eq!(actual.num_rows(), rows);
    let timestamp_output = timestamp_type();
    let output_type = actual.column(0).data_type();
    assert_eq!(
        if program.string_output
            && matches!(output_type, DataType::Struct(fields) if fields[0].name() == "utf8")
        {
            &DataType::Utf8
        } else {
            output_type
        },
        if program.string_output {
            &DataType::Utf8
        } else if program.boolean_output {
            &DataType::Boolean
        } else if program.long_output {
            &DataType::Int64
        } else {
            match program.integer_output.as_deref() {
                Some("TINYINT") => &DataType::Int8,
                Some("SMALLINT") => &DataType::Int16,
                Some("INTEGER") => &DataType::Int32,
                Some("BIGINT") => &DataType::Int64,
                Some("TIMESTAMP_WITH_LOCAL_TIME_ZONE") => &timestamp_output,
                _ => batch.column(0).data_type(),
            }
        }
    );
    let expected = checksum(&actual).wrapping_mul(loops as i64);
    let (diagnostic, allocations) = measure(|| native.run(batch.clone()));
    report(
        "integral_expression",
        batch.columns(),
        diagnostic.columns(),
        allocations,
    );
    for trial in 0..warmups + runs {
        for engine in (0..2).map(|i| (i + trial) % 2) {
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
                let calc = &mut native;
                let start = Instant::now();
                let mut sum = 0i64;
                for _ in 0..loops {
                    sum =
                        sum.wrapping_add(checksum(&black_box(calc.run(black_box(batch.clone())))));
                }
                (start.elapsed().as_nanos(), black_box(sum))
            };
            assert_eq!(digest, expected);
            println!("integral_expression,{element},{nullable},{binary},{rows},{},{trial},{},{},{nanos},{digest}",
                rows*loops,trial<warmups,
                if engine == 0 { "flink" } else if previous { "previous" } else { "native" });
        }
    }
    java!(env, env.call_static_method(class, "close", "()V", &[]));
}

fn json_container_input(ty: &str, rows: usize, nullable: bool) -> ArrayRef {
    let integer = |index: usize| {
        if index % 17 == 0 {
            i32::MIN
        } else {
            (index as i32).wrapping_mul(7919)
        }
    };
    let text = |index: usize| format!("text-{}/\\\"\0\u{1f600}", index % 1024);
    let width: usize = std::env::var("SF_INTEGRAL_CONTAINER_WIDTH")
        .unwrap_or_else(|_| "4".into())
        .parse()
        .unwrap();
    match ty {
        "ARRAY<INT>" => {
            let mut lists = ListBuilder::new(Int32Builder::new());
            for row in 0..rows {
                let valid = !nullable || row % 7 != 0;
                if valid && row % 13 != 0 {
                    for index in row..row + width {
                        lists
                            .values()
                            .append_option((index % 11 != 0).then(|| integer(index)));
                    }
                }
                lists.append(valid);
            }
            Arc::new(lists.finish())
        }
        "ARRAY<VARCHAR(128)>" => {
            let mut lists = ListBuilder::new(StringBuilder::new());
            for row in 0..rows {
                let valid = !nullable || row % 7 != 0;
                if valid && row % 13 != 0 {
                    for index in row..row + width {
                        lists
                            .values()
                            .append_option((index % 11 != 0).then(|| text(index)));
                    }
                }
                lists.append(valid);
            }
            Arc::new(lists.finish())
        }
        "ROW<z INT,a VARCHAR(128)>" => {
            let fields = vec![
                Arc::new(Field::new("z", DataType::Int32, true)),
                Arc::new(Field::new("a", DataType::Utf8, true)),
            ]
            .into();
            let values: Vec<ArrayRef> = vec![
                Arc::new(Int32Array::from_iter(
                    (0..rows).map(|row| (row % 11 != 0).then(|| integer(row))),
                )),
                Arc::new(StringArray::from_iter(
                    (0..rows).map(|row| (row % 13 != 0).then(|| text(row))),
                )),
            ];
            let nulls = nullable
                .then(|| NullBuffer::from((0..rows).map(|row| row % 7 != 0).collect::<Vec<_>>()));
            Arc::new(StructArray::new(fields, values, nulls))
        }
        _ => panic!("unverified JSON container benchmark type {ty}"),
    }
}

fn decimal_epoch_coefficients(precision: u8, scale: i8) -> Vec<i128> {
    let limit = 10_i128.pow(precision as u32);
    let factor = 10_i128.pow(scale as u32);
    let mut values = vec![0, 1, -1, limit - 1, 1 - limit];
    for integer in [
        0,
        -1,
        1,
        -86401,
        951827696,
        1706659200,
        -62167219200,
        253402300799,
        253402300800,
        -62167219200000,
        253402300799999,
        253402300800000,
        i64::MIN,
        i64::MAX,
    ] {
        for wrap in [0_i128, -1, 1] {
            let Some(base) = (i128::from(integer) + (wrap << 64)).checked_mul(factor) else {
                continue;
            };
            for fraction in [-1, 0, 1] {
                if let Some(coefficient) = base.checked_add(fraction).filter(|v| v.abs() < limit) {
                    values.push(coefficient);
                }
            }
        }
    }
    values
}

fn json_text(row: usize, corpus: &str) -> String {
    let texts = [
        String::new(),
        "plain".into(),
        "a/b\\c\"d".into(),
        "\0\t\r\n\x08\x0c\x1f".into(),
        "\\uD800".into(),
        "\u{4e2d}\u{6587}\u{1f600}".into(),
        "{\"a\":1}".into(),
        "0123456789".repeat(64),
    ];

    match corpus {
        "json-query-wide" => {
            let fields: Vec<String> = (0..32)
                .rev()
                .map(|i| format!("\"f{i}\":[{},\"text\",null]", row + i))
                .collect();
            format!("{{\"a\":{{{}}},\"drop\":true}}", fields.join(","))
        }
        "json-query-deep" => {
            format!("{{\"a\":{}{row}{}}}", "{\"n\":".repeat(24), "}".repeat(24))
        }
        "json-query" => match row % 8 {
            0 => r#"{"a":{"z":7,"a":2.5000,"name":"plain"},"drop":0}"#.into(),
            1 => r#"{"a":[1,2.5000,-0,1e10,1e-8],"b":true}"#.into(),
            2 => r#"{"a":{"dup":1,"d\u0075p":2,"x":"a/b\\c\"d"}}"#.into(),
            3 => "{\"a\":{\"\u{4e2d}\u{6587}\":\"\u{1f600}\",\"p\":\"\\uD800\"}}".into(),
            4 => r#"{"a":null}"#.into(),
            5 => r#"{"missing":[]}"#.into(),
            6 => r#"{"a":"text"}"#.into(),
            _ => format!("{{\"a\":{{\"long\":\"{}\"}}}}", "0123456789".repeat(64)),
        },
        _ => texts[row % texts.len()].clone(),
    }
}
