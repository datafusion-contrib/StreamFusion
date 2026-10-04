use arrow::array::{ArrayRef, Float64Array, Int32Array, RecordBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use jni::objects::{JObject, JValue};
use std::sync::Arc;
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn register(vm: &jni::JavaVM, shortcut: bool) -> i32 {
    let mut env = vm.attach_current_thread().unwrap();
    let name = if shortcut {
        "tech/streamfusion/planner/ExactDoubleTruncateFunction"
    } else {
        "tech/streamfusion/bench/ReleasedDoubleTruncate"
    };
    let class = env.find_class(name).unwrap();
    let parameters = env
        .new_object_array(2, "java/lang/Class", JObject::null())
        .unwrap();
    for (index, boxed) in ["java/lang/Double", "java/lang/Integer"]
        .into_iter()
        .enumerate()
    {
        let parameter: JObject = env.find_class(boxed).unwrap().into();
        env.set_object_array_element(&parameters, index as i32, parameter)
            .unwrap();
    }
    let method_name: JObject = env.new_string("eval").unwrap().into();
    let method = env
        .call_method(
            &class,
            "getMethod",
            "(Ljava/lang/String;[Ljava/lang/Class;)Ljava/lang/reflect/Method;",
            &[JValue::Object(&method_name), JValue::Object(&parameters)],
        )
        .unwrap()
        .l()
        .unwrap();
    let types = env.new_int_array(2).unwrap();
    env.set_int_array_region(&types, 0, &[3, 2]).unwrap();
    let result = if shortcut {
        let function = env.new_object(class, "()V", &[]).unwrap();
        env.call_static_method(
            "tech/streamfusion/operator/NativeUdf",
            "register",
            "(Lorg/apache/flink/table/functions/ScalarFunction;Ljava/lang/reflect/Method;[II)I",
            &[
                JValue::Object(&function),
                JValue::Object(&method),
                JValue::Object(&types),
                JValue::Int(3),
            ],
        )
    } else {
        env.call_static_method(
            "tech/streamfusion/operator/NativeUdf",
            "registerBuiltin",
            "(Ljava/lang/reflect/Method;[II)I",
            &[
                JValue::Object(&method),
                JValue::Object(&types),
                JValue::Int(3),
            ],
        )
    };
    result.unwrap().i().unwrap()
}
fn plan(id: i32) -> CalcProgram {
    CalcProgram::new(
        vec![17, 0, 0],
        vec![0, 0, 1],
        vec![2, 0, 0],
        vec![id as i64, 3],
        vec![],
        vec![],
    )
}
fn assert_exact(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.num_rows(), expected.num_rows());
    for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
        let actual = actual.as_any().downcast_ref::<Float64Array>().unwrap();
        let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(
            actual
                .iter()
                .map(|value| value.map(f64::to_bits))
                .eq(expected.iter().map(|value| value.map(f64::to_bits))),
            "DOUBLE result bits or NULL positions differ"
        );
    }
}
fn run(c: &mut Criterion) {
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH")
        .expect("Run bin/bench-native.py to prepare the JVM classpath");
    let property = format!("-Djava.class.path={classpath}");
    let args = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&property)
        .option("-Xms128m")
        .option("-Xmx256m")
        .option("-Dfile.encoding=UTF-8")
        .option("-Duser.timezone=UTC")
        .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .option("-Darrow.enable_unsafe_memory_access=true")
        .option("-Darrow.enable_null_check_for_get=false")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(args).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let reference_id = register(&vm, false);
    let shortcut_id = register(&vm, true);
    let mut env = vm.attach_current_thread().unwrap();
    let generated = env.call_static_method(
        "tech/streamfusion/planner/TruncateBenchmarkFunctions",
        "registerGenerated",
        "()I",
        &[],
    );
    if env.exception_check().unwrap() {
        env.exception_describe().unwrap();
    }
    let generated_id = generated.unwrap().i().unwrap();
    let borrowed = env.call_static_method(
        "tech/streamfusion/planner/TruncateBenchmarkFunctions",
        "registerBorrowed",
        "()I",
        &[],
    );
    if env.exception_check().unwrap() {
        env.exception_describe().unwrap();
    }
    let borrowed_id = borrowed.unwrap().i().unwrap();
    drop(env);
    header();
    let mut group = c.benchmark_group("jvm_truncate");
    for rows in [16, 1024, 16384] {
        for profile in [
            "bounded",
            "decimal_boundary",
            "decimal_ambiguous",
            "outside_domain",
            "large_domain",
            "rounded_subunit",
        ] {
            for nullable in [false, true] {
                let values: ArrayRef = Arc::new(Float64Array::from_iter((0..rows + 1).map(|i| {
                    if nullable && i % 7 == 0 {
                        None
                    } else {
                        let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
                        Some(
                            sign * match profile {
                                "bounded" => 1000.12345 + (i % 10000) as f64,
                                "decimal_boundary" => 1000.5 + (i % 10000) as f64,
                                "decimal_ambiguous" => 1000.1 + (i % 10000) as f64,
                                "large_domain" => 1e12 + 0.12345 + (i % 10000) as f64,
                                "rounded_subunit" => {
                                    [1e-6 - 0.5e-18, 1e-6 + 0.5e-18, 0.5e-18, 1.5e-18][i % 4]
                                }
                                _ => 0.46,
                            },
                        )
                    }
                })));
                let scales: ArrayRef = Arc::new(Int32Array::from_iter((0..rows + 1).map(|i| {
                    if nullable && i % 11 == 0 {
                        None
                    } else {
                        Some(if profile == "rounded_subunit" {
                            6
                        } else if matches!(profile, "decimal_boundary" | "decimal_ambiguous") {
                            1
                        } else {
                            (i % 7) as i32 - 3
                        })
                    }
                })));
                let batch = RecordBatch::try_from_iter([("value", values), ("scale", scales)])
                    .unwrap()
                    .slice(1, rows);
                let expected = plan(reference_id).run(batch.clone());
                assert_exact(&plan(shortcut_id).run(batch.clone()), &expected);
                assert_exact(&plan(generated_id).run(batch.clone()), &expected);
                assert_exact(&plan(borrowed_id).run(batch.clone()), &expected);
                for (name, id) in [
                    ("released_flink", reference_id),
                    ("bounded_shortcut", shortcut_id),
                    ("generated_flink", generated_id),
                    ("borrowed_shortcut", borrowed_id),
                ] {
                    let mut calc = plan(id);
                    assert_exact(&calc.run(batch.clone()), &expected);
                    let label = format!("{name}/{rows}/{profile}/nulls={nullable}");
                    let (out, allocations) = measure(|| calc.run(batch.clone()));
                    report(&label, batch.columns(), out.columns(), allocations);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(calc.run(std::hint::black_box(batch.clone())))
                        })
                    });
                }
            }
        }
    }
    group.finish();
    let mut env = vm.attach_current_thread().unwrap();
    for id in [reference_id, shortcut_id, generated_id, borrowed_id] {
        env.call_static_method(
            "tech/streamfusion/operator/NativeUdf",
            "unregister",
            "(I)V",
            &[JValue::Int(id)],
        )
        .unwrap();
    }
}
criterion_group!(benches, run);
criterion_main!(benches);
