use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use jni::objects::{GlobalRef, JValue};
use std::sync::Arc;
use streamfusion::bench::{PersistentWindowJoin, WindowJoin, WindowRankState};

fn time(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis()
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
fn canonical_rank(batch: &RecordBatch) -> RecordBatch {
    let indices = arrow::compute::lexsort_to_indices(
        &[
            arrow::compute::SortColumn {
                values: batch.column(2).clone(),
                options: None,
            },
            arrow::compute::SortColumn {
                values: batch.column(4).clone(),
                options: None,
            },
        ],
        None,
    )
    .unwrap();
    RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|column| arrow::compute::take(column, &indices, None).unwrap())
            .collect(),
    )
    .unwrap()
}

fn run(c: &mut Criterion) {
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH")
        .expect("Run bin/bench-native.py to prepare JVM classes");
    let property = format!("-Djava.class.path={classpath}");
    let args = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&property)
        .option("-Xmx512m")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(args).unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut group = c.benchmark_group("window_consumers_calendar");
    for (zone, end, deadline) in [
        (
            "America/Los_Angeles",
            time("2024-03-10T02:30:00Z"),
            time("2024-03-10T10:00:00Z") + 1,
        ),
        (
            "America/Los_Angeles",
            time("2024-11-03T02:00:00Z"),
            time("2024-11-03T10:00:00Z"),
        ),
        ("Asia/Shanghai", time("1970-01-01T08:00:00Z"), 0),
    ] {
        let callback = callback(&vm, zone);
        for rows in [64, 4096] {
            group.throughput(Throughput::Elements(rows as u64));
            for width in [0, 256] {
                let keys = rows;
                let names = Arc::new(StringArray::from_iter((0..rows).map(|row| {
                    (row % 5 != 0).then(|| format!("key{}-{}", row % keys, "x".repeat(width)))
                }))) as ArrayRef;
                let values = Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| row as i64),
                )) as ArrayRef;
                let starts = Arc::new(Int64Array::from(vec![end - 3_600_000; rows])) as ArrayRef;
                let ends = Arc::new(Int64Array::from(vec![end; rows])) as ArrayRef;
                let join_batch = RecordBatch::try_from_iter(vec![
                    ("key", names.clone()),
                    ("value", values.clone()),
                    ("start", starts.clone()),
                    ("end", ends.clone()),
                ])
                .unwrap();
                let rank_batch = RecordBatch::try_from_iter(vec![
                    ("start", starts),
                    ("end", ends),
                    ("key", names),
                    ("value", values),
                ])
                .unwrap();
                for rocks in [false, true] {
                    let label = format!("{zone}/end{end}/rows{rows}/width{width}/rocks{rocks}");
                    let mut control = WindowJoin::new(
                        vec![0],
                        vec![0],
                        2,
                        3,
                        2,
                        3,
                        join_batch.schema(),
                        join_batch.schema(),
                    );
                    control.calendar(callback.clone());
                    control.push_left(join_batch.clone());
                    control.push_right(join_batch.clone());
                    assert_eq!(control.flush(deadline - 1).num_rows(), 0);
                    let expected = control.flush(deadline).num_rows();
                    assert!(expected > 0);
                    group.bench_function(format!("join/{label}"), |b| {
                        b.iter_batched(
                            || {
                                let directory = tempfile::tempdir().unwrap();
                                if rocks {
                                    let mut op = PersistentWindowJoin::new(
                                        directory.path().to_str().unwrap(),
                                        join_batch.schema(),
                                        include_str!("fixtures/rocks-options.json"),
                                    );
                                    op.calendar(callback.clone());
                                    (directory, None, Some(op))
                                } else {
                                    let mut op = WindowJoin::new(
                                        vec![0],
                                        vec![0],
                                        2,
                                        3,
                                        2,
                                        3,
                                        join_batch.schema(),
                                        join_batch.schema(),
                                    );
                                    op.calendar(callback.clone());
                                    (directory, Some(op), None)
                                }
                            },
                            |(_directory, memory, persistent)| {
                                let output = if let Some(mut op) = memory {
                                    op.push_left(join_batch.clone());
                                    op.push_right(join_batch.clone());
                                    op.flush(deadline - 1);
                                    op.flush(deadline)
                                } else {
                                    let mut op = persistent.unwrap();
                                    op.push(&join_batch, true);
                                    op.push(&join_batch, false);
                                    op.flush(deadline - 1);
                                    op.flush(deadline)
                                };
                                assert_eq!(output.num_rows(), expected);
                                black_box(output)
                            },
                            BatchSize::SmallInput,
                        )
                    });
                    for limit in [1, 4] {
                        let mut control = WindowRankState::memory(limit, limit == 1);
                        control.calendar(callback.clone());
                        control.push(&rank_batch);
                        assert_eq!(control.flush(deadline - 1).num_rows(), 0);
                        let expected = control.flush(deadline).num_rows();
                        group.bench_function(format!("rank{limit}/{label}"), |b| {
                            b.iter_batched(
                                || {
                                    let directory = tempfile::tempdir().unwrap();
                                    let mut op = if rocks {
                                        WindowRankState::new(
                                            directory.path().to_str().unwrap(),
                                            rank_batch.schema(),
                                            include_str!("fixtures/rocks-options.json"),
                                            limit,
                                            limit == 1,
                                        )
                                    } else {
                                        WindowRankState::memory(limit, limit == 1)
                                    };
                                    op.calendar(callback.clone());
                                    (directory, op)
                                },
                                |(_directory, mut op)| {
                                    op.push(&rank_batch);
                                    op.flush(deadline - 1);
                                    let output = op.flush(deadline);
                                    assert_eq!(output.num_rows(), expected);
                                    black_box(output)
                                },
                                BatchSize::SmallInput,
                            )
                        });
                    }
                }
            }
        }
    }
    for rows in [64, 4096] {
        for keys in [8, rows] {
            let values = streamfusion_bridge::timestamp::timestamp_array((0..rows).map(|row| {
                (row % 7 != 0).then(|| {
                    streamfusion_bridge::timestamp::TimestampValue::new(
                        row as i64 - 2048,
                        (row % 1_000_000) as u32,
                    )
                    .unwrap()
                })
            }));
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "start",
                    Arc::new(Int64Array::from(vec![0; rows])) as ArrayRef,
                ),
                (
                    "end",
                    Arc::new(Int64Array::from(vec![100; rows])) as ArrayRef,
                ),
                (
                    "key",
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|row| format!("key{}", row % keys)),
                    )) as ArrayRef,
                ),
                ("value", Arc::new(values) as ArrayRef),
            ])
            .unwrap();
            for limit in [1, 4] {
                let mut control = WindowRankState::memory(limit, limit == 1);
                control.push(&batch);
                let expected = canonical_rank(&control.flush(100));
                for rocks in [false, true] {
                    group.bench_function(
                        format!(
                            "component_timestamp/rows{rows}/keys{keys}/limit{limit}/rocks{rocks}"
                        ),
                        |b| {
                            b.iter_batched(
                                || {
                                    let directory = tempfile::tempdir().unwrap();
                                    let op = if rocks {
                                        WindowRankState::new(
                                            directory.path().to_str().unwrap(),
                                            batch.schema(),
                                            include_str!("fixtures/rocks-options.json"),
                                            limit,
                                            limit == 1,
                                        )
                                    } else {
                                        WindowRankState::memory(limit, limit == 1)
                                    };
                                    (directory, op)
                                },
                                |(_directory, mut op)| {
                                    op.push(&batch.slice(0, rows / 2));
                                    op.push(&batch.slice(rows / 2, rows / 2));
                                    let output = op.flush(100);
                                    assert_eq!(canonical_rank(&output), expected);
                                    black_box(output)
                                },
                                BatchSize::SmallInput,
                            )
                        },
                    );
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, run);
criterion_main!(benches);
