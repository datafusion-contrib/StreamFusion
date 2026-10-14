use arrow::array::{
    Array, ArrayRef, Int8Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::compute::concat_batches;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use jni::objects::{GlobalRef, JValue};
use std::sync::Arc;
use streamfusion::bench::assign_calendar_windows;

fn callback(vm: &jni::JavaVM, zone: &str) -> GlobalRef {
    let mut env = vm.attach_current_thread().unwrap();
    let zone = env.new_string(zone).unwrap();
    let object = env
        .new_object(
            "tech/streamfusion/operator/WindowCalendar",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&zone)],
        )
        .unwrap();
    env.new_global_ref(object).unwrap()
}
fn time(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis()
}
fn fixture(zone: &str, rows: usize, width: usize, nulls: bool) -> RecordBatch {
    let times = if zone == "Asia/Shanghai" {
        [
            time("1969-12-31T23:30:00Z"),
            time("1900-01-01T00:00:00Z"),
            time("2024-01-01T00:00:00Z"),
            time("1991-09-15T00:00:00Z"),
        ]
    } else {
        [
            time("2024-03-10T09:50:00Z"),
            time("2024-03-10T10:10:00Z"),
            time("2024-11-03T08:30:00Z"),
            time("2024-11-03T09:30:00Z"),
        ]
    };
    RecordBatch::try_from_iter(vec![
        (
            "payload",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| format!("row{row}-{}", "x".repeat(width))),
            )) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(TimestampMillisecondArray::from_iter(
                (0..rows).map(|row| (!nulls || row % 5 != 0).then_some(times[row % 4])),
            )) as ArrayRef,
        ),
        (
            "$row_kind$",
            Arc::new(Int8Array::from_iter_values(
                (0..rows).map(|row| (row % 4) as i8),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}
fn run(c: &mut Criterion) {
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH")
        .expect("Run bin/bench-native.py to prepare JVM classes");
    let property = format!("-Djava.class.path={classpath}");
    let args = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&property)
        .option("-Xms128m")
        .option("-Xmx512m")
        .option("-Duser.timezone=UTC")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(args).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut group = c.benchmark_group("regional_window_tvf_calendar");
    for zone in ["America/Los_Angeles", "Asia/Shanghai"] {
        let callback = callback(&vm, zone);
        for size in [1, 3_600_000, 86_400_000] {
            for shape in ["tumble", "hop", "cumulate"] {
                let slide = if shape == "tumble" || size == 1 {
                    size
                } else {
                    size / 4
                };
                let cumulative = shape == "cumulate";
                for rows in [64, 4096] {
                    group.throughput(Throughput::Elements(rows as u64));
                    for width in [0, 256] {
                        for nulls in [false, true] {
                            let batch = fixture(zone, rows, width, nulls);
                            for proctime in [false, true] {
                                let now = proctime.then_some(time("2024-03-10T09:50:00Z"));
                                let whole = assign_calendar_windows(
                                    &batch,
                                    1,
                                    size,
                                    slide,
                                    cumulative,
                                    now,
                                    callback.clone(),
                                );
                                let parts = (0..rows)
                                    .step_by(17)
                                    .map(|start| {
                                        assign_calendar_windows(
                                            &batch.slice(start, 17.min(rows - start)),
                                            1,
                                            size,
                                            slide,
                                            cumulative,
                                            now,
                                            callback.clone(),
                                        )
                                    })
                                    .collect::<Vec<_>>();
                                assert_eq!(whole, concat_batches(&whole.schema(), &parts).unwrap());
                                let payload = whole
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                let tags = whole
                                    .column(5)
                                    .as_any()
                                    .downcast_ref::<Int8Array>()
                                    .unwrap();
                                for row in 0..whole.num_rows() {
                                    let index = payload
                                        .value(row)
                                        .split('-')
                                        .next()
                                        .unwrap()
                                        .strip_prefix("row")
                                        .unwrap()
                                        .parse::<usize>()
                                        .unwrap();
                                    assert_eq!(tags.value(row), (index % 4) as i8);
                                    assert_eq!(
                                        whole.column(1).is_null(row),
                                        batch.column(1).is_null(index)
                                    );
                                }
                                group.bench_function(format!("{zone}_size{size}_{shape}_rows{rows}_width{width}_nulls{nulls}_proctime{proctime}"),|b|b.iter(||black_box(assign_calendar_windows(&batch,1,size,slide,cumulative,now,callback.clone()))));
                            }
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, run);
criterion_main!(benches);
