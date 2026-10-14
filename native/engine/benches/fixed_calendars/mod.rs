use super::*;
const DAY: i64 = 86_400_000;

fn fixture(rows: usize, keys: usize, base: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| base + (row % 7) as i64 * DAY / 6 + 1),
            )) as ArrayRef,
        ),
        (
            "value0",
            Arc::new(Int64Array::from_iter((0..rows).map(|row| {
                (row % 5 != 0 && (keys == 1 || row % keys != 0)).then_some(1)
            }))) as ArrayRef,
        ),
        (
            "key0",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % keys) as i64),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

pub(super) fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_calendar_windows");
    for rows in [64, 4096] {
        group.throughput(Throughput::Elements(rows as u64));
        for keys in [1, 64] {
            for offset in [28_800_000, -28_800_000, 20_700_000] {
                for base in [-2 * DAY, 1_704_067_200_000] {
                    let batch = fixture(rows, keys, base);
                    for (shape, size, slide, cumulative) in [
                        ("tumble", DAY, DAY, false),
                        ("hop", DAY, DAY / 4, false),
                        ("cumulate", DAY, DAY / 4, true),
                    ] {
                        let close = base + 3 * DAY;
                        let mut whole = Tumbling::fixed_calendar(size, slide, cumulative, offset);
                        whole.update(&batch);
                        let expected = whole.flush(close);
                        let mut fragmented =
                            Tumbling::fixed_calendar(size, slide, cumulative, offset);
                        for start in (0..rows).step_by(17) {
                            fragmented.update(&batch.slice(start, 17.min(rows - start)));
                            fragmented = Tumbling::restore_fixed_calendar(
                                size,
                                slide,
                                cumulative,
                                &fragmented.snapshot_fixed_calendar(),
                            );
                        }
                        assert_eq!(
                            expected,
                            fragmented.flush(close),
                            "fixed grid snapshot continuation"
                        );
                        let mut local = Tumbling::fixed_calendar(slide, slide, false, offset);
                        local.update_local(&batch);
                        let partial = local.drain_partial();
                        let mut global = Tumbling::fixed_calendar(size, slide, cumulative, offset);
                        global.update_partial(&partial);
                        assert_eq!(
                            expected,
                            global.flush(close),
                            "fixed grid local/global partials"
                        );
                        group.bench_function(
                            format!("{shape}_rows{rows}_keys{keys}_offset{offset}_base{base}"),
                            |b| {
                                b.iter_batched_ref(
                                    || Tumbling::fixed_calendar(size, slide, cumulative, offset),
                                    |operator| {
                                        operator.update(&batch);
                                        black_box(operator.flush(close))
                                    },
                                    BatchSize::SmallInput,
                                )
                            },
                        );
                    }
                }
            }
        }
    }
    group.finish();
}
