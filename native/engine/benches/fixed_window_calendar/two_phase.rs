use super::*;

fn execute(
    global: &mut Tumbling,
    callback: &GlobalRef,
    profile: &Profile,
    batch: &RecordBatch,
    size: i64,
    slide: i64,
    cumulative: bool,
    checkpoint: bool,
) -> (RecordBatch, usize) {
    let mut local = Tumbling::fixed_calendar(slide, slide, false, 0);
    local.calendar(callback.clone());
    local.update_local(batch);
    assert_eq!(
        local.local_buffer_ready(profile.first - 1),
        profile.name != "history" && size != slide
    );
    assert!(local.local_buffer_ready(profile.last - 1));
    global.update_partial(&local.drain_partial());
    if checkpoint {
        *global = Tumbling::restore_fixed_calendar(
            size,
            slide,
            cumulative,
            &global.snapshot_fixed_calendar(),
        );
        global.calendar(callback.clone());
    }
    let first = global.flush(profile.first);
    let n = first.num_rows();
    let last = global.flush(profile.last);
    local.update_local(batch);
    global.update_partial(&local.drain_partial());
    assert_eq!(
        global.flush(profile.last).num_rows(),
        0,
        "late partials cannot revive a fired window"
    );
    (concat_batches(&first.schema(), &[first, last]).unwrap(), n)
}

fn historical_expected(
    profile: &Profile,
    shape: &str,
    rows: usize,
    keys: usize,
    width: usize,
    nulls: bool,
) -> Vec<((String, i64, i64), i64)> {
    let mut counts = vec![[0i64; 2]; keys];
    let mut present = vec![[false; 2]; keys];
    for row in 0..rows {
        let key = row % keys;
        let phase = (row / keys) % 2;
        present[key][phase] = true;
        if !nulls || (row % 5 != 0 && (keys == 1 || key != 0)) {
            counts[key][phase] += 1;
        }
    }
    let first = profile.times[0] + 1;
    let mut expected = Vec::new();
    for key in 0..keys {
        let name = format!("key{key}-{}", "x".repeat(width));
        let mut emit = |end, count| {
            expected.push((
                (
                    name.clone(),
                    if shape == "hop" { end - 2 } else { first - 2 },
                    end,
                ),
                count,
            ))
        };
        if shape == "hop" {
            if present[key][1] {
                emit(first + 1, counts[key][0] + counts[key][1]);
                emit(first + 2, counts[key][1]);
            } else {
                emit(first, counts[key][0]);
                emit(first + 1, counts[key][0]);
            }
        } else {
            if present[key][1] {
                emit(first + 1, counts[key][1]);
            }
            emit(first, counts[key][0]);
            emit(first + 1, counts[key][0]);
        }
    }
    expected
}

pub(super) fn bench(c: &mut Criterion, vm: &jni::JavaVM) {
    let mut group = c.benchmark_group("regional_two_phase_window_calendar");
    for profile in profiles() {
        let callback = callback(vm, profile.zone);
        for shape in ["tumble", "hop", "cumulate"] {
            let size = if profile.name == "history" {
                match shape {
                    "hop" => 2,
                    "cumulate" => 3,
                    _ => 1,
                }
            } else {
                DAY
            };
            let slide = if profile.name == "history" {
                1
            } else if shape == "tumble" {
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
                                fixture(&profile, shape, rows, keys, width, nulls, true);
                            for rocks in [false, true] {
                                if rocks && !cfg!(feature = "rocksdb-state") {
                                    continue;
                                }
                                for checkpoint in [false, true] {
                                    if rocks && checkpoint {
                                        continue;
                                    }
                                    let setup = || {
                                        let directory = rocks.then(|| tempfile::tempdir().unwrap());
                                        let mut global =
                                            Tumbling::fixed_calendar(size, slide, cumulative, 0);
                                        #[cfg(feature = "rocksdb-state")]
                                        if let Some(directory) = &directory {
                                            global = Tumbling::persistent_fixed_calendar(
                                                directory.path().join("db").to_str().unwrap(),
                                                include_str!("../fixtures/rocks-options.json"),
                                                size,
                                                slide,
                                                cumulative,
                                            );
                                        }
                                        global.calendar(callback.clone());
                                        (global, directory)
                                    };
                                    let (out, first) = execute(
                                        &mut setup().0,
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
                                    let names = out
                                        .column(0)
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .unwrap();
                                    let starts = out
                                        .column(1)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap();
                                    let ends = out
                                        .column(2)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap();
                                    let counts = out
                                        .column(3)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap();
                                    let mut actual = (0..out.num_rows())
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
                                        .collect::<Vec<_>>();
                                    let mut expected =
                                        if profile.name == "history" && shape != "tumble" {
                                            historical_expected(
                                                &profile, shape, rows, keys, width, nulls,
                                            )
                                        } else {
                                            expected.clone().into_iter().collect()
                                        };
                                    actual.sort();
                                    expected.sort();
                                    assert_eq!(actual, expected);
                                    group.bench_function(format!("{}_{}_rows{rows}_keys{keys}_width{width}_nulls{nulls}_checkpoint{checkpoint}_rocks{rocks}", profile.name, shape), |b| {
                                    b.iter_batched_ref(setup, |(global, _)| black_box(execute(global, &callback, &profile, &batch, size, slide, cumulative, checkpoint)), BatchSize::SmallInput)
                                });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
