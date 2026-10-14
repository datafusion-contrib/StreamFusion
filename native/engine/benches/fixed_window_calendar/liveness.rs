use super::*;
use arrow::array::Int8Array;

pub(super) fn bench(c: &mut Criterion, vm: &jni::JavaVM) {
    let callback = callback(vm, "America/Los_Angeles");
    let first = time("2024-03-10T13:00:00Z");
    let zero = time("2024-03-10T19:00:00Z");
    let later = time("2024-03-11T07:00:00Z");
    let step = DAY / 4;
    let mut group = c.benchmark_group("regional_window_live_count");
    for cumulative in [false, true] {
        let size = step * if cumulative { 3 } else { 2 };
        for rows in [64, 4096] {
            for keys in [1, 64] {
                for width in [0, 256] {
                    let batch = RecordBatch::try_from_iter(vec![
                        (
                            "ts",
                            Arc::new(Int64Array::from_iter_values((0..rows * 2).map(|row| {
                                time(if row < rows {
                                    "2024-03-10T01:00:00Z"
                                } else {
                                    "2024-03-10T07:00:00Z"
                                })
                            }))) as ArrayRef,
                        ),
                        (
                            "key0",
                            Arc::new(StringArray::from_iter_values(
                                (0..rows * 2)
                                    .map(|row| format!("key{}-{}", row % keys, "x".repeat(width))),
                            )) as ArrayRef,
                        ),
                        (
                            "value0",
                            Arc::new(Int64Array::from_value(1, rows * 2)) as ArrayRef,
                        ),
                        (
                            "$row_kind$",
                            Arc::new(Int8Array::from_iter_values((0..rows * 2).map(|row| {
                                if row < rows {
                                    0
                                } else {
                                    3
                                }
                            }))) as ArrayRef,
                        ),
                    ])
                    .unwrap();
                    for checkpoint in [false, true] {
                        let setup = || {
                            let mut operator =
                                Tumbling::retracting_calendar(size, step, cumulative);
                            operator.calendar(callback.clone());
                            operator
                        };
                        let execute = |operator: &mut Tumbling| {
                            operator.update(&batch);
                            let out = operator.flush(first);
                            assert_eq!(out.num_rows(), keys);
                            let counts =
                                out.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
                            assert!(counts
                                .values()
                                .iter()
                                .all(|count| *count == (rows / keys) as i64));
                            if checkpoint {
                                *operator = Tumbling::restore_retracting_calendar(
                                    size,
                                    step,
                                    cumulative,
                                    &operator.snapshot_fixed_calendar(),
                                );
                                operator.calendar(callback.clone());
                            }
                            assert_eq!(operator.flush(zero).num_rows(), 0);
                            assert_eq!(operator.flush(later).num_rows(), 0);
                            out
                        };
                        execute(&mut setup());
                        group.throughput(Throughput::Elements((rows * 2) as u64));
                        group.bench_function(format!("cumulate{cumulative}_rows{rows}_keys{keys}_width{width}_checkpoint{checkpoint}"), |b| b.iter_batched_ref(setup, |operator| black_box(execute(operator)), BatchSize::SmallInput));
                    }
                }
            }
        }
    }
    group.finish();
}
