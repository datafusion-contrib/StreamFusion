use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use jni::objects::{GlobalRef, JValue};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::Tumbling;
#[path = "fixed_window_calendar/liveness.rs"]
mod liveness;
#[path = "fixed_window_calendar/two_phase.rs"]
mod two_phase;

const DAY: i64 = 86_400_000;
fn time(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis()
}
struct Profile {
    name: &'static str,
    zone: &'static str,
    day: i64,
    times: [i64; 2],
    first: i64,
    last: i64,
}
fn profiles() -> Vec<Profile> {
    vec![
        Profile {
            name: "gap",
            zone: "America/Los_Angeles",
            day: time("2024-03-10T00:00:00Z"),
            times: [time("2024-03-10T01:30:00Z"), time("2024-03-10T03:30:00Z")],
            first: time("2024-03-10T13:00:00Z"),
            last: time("2024-03-12T12:00:00Z"),
        },
        Profile {
            name: "overlap",
            zone: "America/Los_Angeles",
            day: time("2024-11-03T00:00:00Z"),
            times: [time("2024-11-03T01:30:00Z"); 2],
            first: time("2024-11-03T14:00:00Z"),
            last: time("2024-11-05T12:00:00Z"),
        },
        Profile {
            name: "negative",
            zone: "Asia/Shanghai",
            day: 0,
            times: [time("1970-01-01T07:30:00Z"), time("1970-01-01T07:45:00Z")],
            first: time("1970-01-01T04:00:00Z"),
            last: time("1970-01-03T00:00:00Z"),
        },
        Profile {
            name: "history",
            zone: "America/Los_Angeles",
            day: 0,
            times: [
                time("1883-11-18T12:07:01.999Z"),
                time("1883-11-18T12:07:02Z"),
            ],
            first: time("1883-11-18T20:07:02Z") + 1,
            last: time("1883-11-18T20:59:59.999Z") + 1,
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
    shape: &str,
    rows: usize,
    keys: usize,
    width: usize,
    nulls: bool,
    two_phase: bool,
) -> (RecordBatch, Expected) {
    let mut names = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    let mut expected = Expected::new();
    for row in 0..rows {
        let key = row % keys;
        let name = format!("key{key}-{}", "x".repeat(width));
        let phase = (row / keys) % 2;
        let timestamp = profile.times[phase];
        let present = !nulls || (row % 5 != 0 && (keys == 1 || key != 0));
        let windows: Vec<(i64, i64)> = if profile.name == "history" {
            if two_phase && shape == "hop" {
                vec![(timestamp - 1, timestamp + 1), (timestamp, timestamp + 2)]
            } else if two_phase && shape == "cumulate" {
                let base = profile.times[0] - 1;
                if phase == 0 {
                    vec![(base, profile.times[0] + 1), (base, profile.times[0] + 2)]
                } else {
                    vec![(base, profile.times[0] + 2)]
                }
            } else {
                vec![(timestamp, timestamp + 1)]
            }
        } else if shape == "tumble" {
            vec![(profile.day, profile.day + DAY)]
        } else if shape == "hop" {
            let first = if profile.name == "negative" { 2 } else { 1 };
            (first..first + 4)
                .map(|step| {
                    (
                        profile.day + step * DAY / 4 - DAY,
                        profile.day + step * DAY / 4,
                    )
                })
                .collect()
        } else {
            let first = if profile.name == "negative" { 2 } else { 1 };
            (first..=4)
                .map(|step| (profile.day, profile.day + step * DAY / 4))
                .collect()
        };
        for (start, end) in windows {
            *expected.entry((name.clone(), start, end)).or_default() += i64::from(present);
        }
        names.push(name);
        timestamps.push(timestamp);
        values.push(present.then_some(1));
    }
    (
        RecordBatch::try_from_iter(vec![
            ("key0", Arc::new(StringArray::from(names)) as ArrayRef),
            ("ts", Arc::new(Int64Array::from(timestamps)) as ArrayRef),
            ("value0", Arc::new(Int64Array::from(values)) as ArrayRef),
        ])
        .unwrap(),
        expected,
    )
}
fn execute(
    operator: &mut Tumbling,
    callback: &GlobalRef,
    profile: &Profile,
    batch: &RecordBatch,
    size: i64,
    slide: i64,
    cumulative: bool,
    checkpoint: bool,
) -> (RecordBatch, usize) {
    operator.update(batch);
    if checkpoint {
        *operator = Tumbling::restore_fixed_calendar(
            size,
            slide,
            cumulative,
            &operator.snapshot_fixed_calendar(),
        );
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
    for cumulative in [false, true] {
        let profile = profiles().remove(0);
        let callback = callback(&vm, profile.zone);
        let (batch, _) = fixture(
            &profile,
            if cumulative { "cumulate" } else { "hop" },
            4096,
            64,
            256,
            true,
            false,
        );
        let mut operator = Tumbling::fixed_calendar(DAY, DAY / 4, cumulative, 0)
            .account_calendar_state(256 * 1024 * 1024);
        operator.calendar(callback);
        operator.update(&batch);
        operator.assert_calendar_accounting();
        for tick in (0..=48).map(|hour| profile.first + hour * 3_600_000) {
            operator.flush(tick);
            operator.assert_calendar_accounting();
        }
    }
    let mut group = c.benchmark_group("regional_fixed_window_calendar");
    for profile in profiles() {
        let callback = callback(&vm, profile.zone);
        for shape in ["tumble", "hop", "cumulate"] {
            let size = if profile.name == "history" { 1 } else { DAY };
            let slide = if shape == "tumble" || size == 1 {
                size
            } else {
                size / 4
            };
            let cumulative = shape == "cumulate";
            for rows in [64, 4096] {
                group.throughput(Throughput::Elements(rows as u64));
                for keys in [1, 64] {
                    for width in [0, 256] {
                        for nulls in [false, true] {
                            let (batch, expected) =
                                fixture(&profile, shape, rows, keys, width, nulls, false);
                            for checkpoint in [false, true] {
                                let setup = || {
                                    let mut operator =
                                        Tumbling::fixed_calendar(size, slide, cumulative, 0);
                                    operator.calendar(callback.clone());
                                    operator
                                };
                                let (result, first) = execute(
                                    &mut setup(),
                                    &callback,
                                    &profile,
                                    &batch,
                                    size,
                                    slide,
                                    cumulative,
                                    checkpoint,
                                );
                                let expected_first = if profile.name == "history" {
                                    if rows > keys {
                                        keys
                                    } else {
                                        0
                                    }
                                } else if shape == "tumble" {
                                    0
                                } else {
                                    keys
                                };
                                assert_eq!(first, expected_first);
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
                                group.bench_function(format!("{}_{}_rows{rows}_keys{keys}_width{width}_nulls{nulls}_checkpoint{checkpoint}",profile.name,shape),|b|{b.iter_batched_ref(setup,|operator|black_box(execute(operator,&callback,&profile,&batch,size,slide,cumulative,checkpoint)),BatchSize::SmallInput)});
                            }
                        }
                    }
                }
            }
        }
    }
    group.finish();
    two_phase::bench(c, &vm);
    liveness::bench(c, &vm);
}
criterion_group!(benches, run);
criterion_main!(benches);
