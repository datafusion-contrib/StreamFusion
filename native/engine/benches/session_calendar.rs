use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use jni::objects::{GlobalRef, JValue};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::Session;

fn time(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis()
}

struct Profile {
    name: &'static str,
    zone: &'static str,
    gap: i64,
    times: [i64; 2],
    first: i64,
    last: i64,
}

fn profiles() -> Vec<Profile> {
    vec![
        Profile {
            name: "overlap",
            zone: "America/Los_Angeles",
            gap: 900_000,
            times: [time("2024-11-03T01:20:00Z"), time("2024-11-03T01:25:00Z")],
            first: time("2024-11-03T08:34:59.999Z") + 1,
            last: time("2024-11-03T09:39:59.999Z") + 1,
        },
        Profile {
            name: "gap",
            zone: "America/Los_Angeles",
            gap: 1_200_000,
            times: [time("2024-03-10T01:50:00Z"), time("2024-03-10T03:05:00Z")],
            first: time("2024-03-10T10:00:00Z") + 1,
            last: time("2024-03-10T10:24:59.999Z") + 1,
        },
        Profile {
            name: "history",
            zone: "America/Los_Angeles",
            gap: 1,
            times: [
                time("1883-11-18T12:07:01.999Z"),
                time("1883-11-18T12:07:02Z"),
            ],
            first: time("1883-11-18T20:07:02Z") + 1,
            last: time("1883-11-18T20:59:59.999Z") + 1,
        },
        Profile {
            name: "negative",
            zone: "Asia/Shanghai",
            gap: 500,
            times: [
                time("1970-01-01T07:59:59Z"),
                time("1970-01-01T07:59:59.200Z"),
            ],
            first: -301,
            last: -300,
        },
    ]
}

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

type Expected = BTreeMap<(String, i64, i64), i64>;

fn fixture(
    profile: &Profile,
    rows: usize,
    keys: usize,
    width: usize,
    nulls: bool,
) -> (RecordBatch, Expected) {
    let mut names = Vec::new();
    let mut times = Vec::new();
    let mut values = Vec::new();
    let mut expected = Expected::new();
    for row in 0..rows {
        let key = row % keys;
        let name = format!("key{key}-{}", "x".repeat(width));
        let phase = if profile.name == "history" {
            key % 2
        } else {
            (row / keys) % 2
        };
        let timestamp = profile.times[phase];
        let present = !nulls || (row % 5 != 0 && (keys == 1 || key != 0));
        let (start, end) = if profile.name == "gap" || profile.name == "history" {
            (timestamp, timestamp + profile.gap)
        } else {
            (
                profile.times[0],
                profile.times[usize::from(rows > keys)] + profile.gap,
            )
        };
        *expected.entry((name.clone(), start, end)).or_default() += i64::from(present);
        names.push(name);
        times.push(timestamp);
        values.push(present.then_some(1));
    }
    (
        RecordBatch::try_from_iter(vec![
            ("key0", Arc::new(StringArray::from(names)) as ArrayRef),
            ("ts", Arc::new(Int64Array::from(times)) as ArrayRef),
            ("value0", Arc::new(Int64Array::from(values)) as ArrayRef),
        ])
        .unwrap(),
        expected,
    )
}

fn execute(
    operator: &mut Session,
    callback: &GlobalRef,
    profile: &Profile,
    batch: &RecordBatch,
    checkpoint: bool,
) -> (RecordBatch, usize) {
    operator.update(batch);
    if checkpoint {
        *operator = Session::restore(profile.gap, 0, vec![3], &operator.snapshot());
        operator.calendar(callback.clone());
    }
    let first = operator.flush(profile.first);
    let n = first.num_rows();
    let last = operator.flush(profile.last);
    (concat_batches(&first.schema(), &[first, last]).unwrap(), n)
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
    let mut group = c.benchmark_group("regional_session_calendar");
    for profile in profiles() {
        let callback = callback(&vm, profile.zone);
        for rows in [64, 4096] {
            group.throughput(Throughput::Elements(rows as u64));
            for keys in [1, 64] {
                for width in [0, 256] {
                    for nulls in [false, true] {
                        let (batch, expected) = fixture(&profile, rows, keys, width, nulls);
                        for checkpoint in [false, true] {
                            let setup = || {
                                let mut operator = Session::new(profile.gap, 0, vec![3]);
                                operator.calendar(callback.clone());
                                operator
                            };
                            let (result, first) =
                                execute(&mut setup(), &callback, &profile, &batch, checkpoint);
                            assert_eq!(
                                first,
                                match profile.name {
                                    "gap" => keys,
                                    "history" => keys / 2,
                                    "negative" if rows == keys => keys,
                                    _ => 0,
                                }
                            );
                            let names = result
                                .column(0)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            let starts = result
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            let ends = result
                                .column(2)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            let counts = result
                                .column(3)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            let actual = (0..result.num_rows())
                                .map(|row| {
                                    (
                                        (
                                            names.value(row).to_owned(),
                                            starts.value(row),
                                            ends.value(row),
                                        ),
                                        counts.value(row),
                                    )
                                })
                                .collect::<Expected>();
                            assert_eq!(actual, expected);
                            group.bench_function(format!("{}_rows{rows}_keys{keys}_width{width}_nulls{nulls}_checkpoint{checkpoint}", profile.name), |b| {
                                b.iter_batched_ref(setup, |operator| black_box(execute(operator, &callback, &profile, &batch, checkpoint)), BatchSize::SmallInput)
                            });
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
