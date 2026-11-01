use super::*;

fn create(keyless: bool, counted: bool) -> SessionAggregator {
    let mut value = SessionAggregator::new(
        1000,
        vec![0, 0],
        vec![
            if counted {
                crate::aggregates::COUNT_DISTINCT_WITH_COUNTS
            } else {
                7
            },
            3,
        ],
    )
    .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] });
    value.bind_canonical_distinct(0).unwrap();
    value
}

fn input(keyless: bool, times: &[i64], values: &[Option<i64>]) -> RecordBatch {
    let mut columns: Vec<(String, ArrayRef)> = vec![
        ("ts".into(), Arc::new(Int64Array::from(times.to_vec()))),
        ("value0".into(), Arc::new(Int64Array::from(values.to_vec()))),
        (
            "value1".into(),
            Arc::new(Int64Array::from(vec![Some(1); times.len()])),
        ),
    ];
    if !keyless {
        columns.push((
            "key0".into(),
            Arc::new(Int64Array::from(vec![Some(7); times.len()])),
        ));
    }
    RecordBatch::try_from_iter(columns).unwrap()
}

fn copy(source: &mut SessionAggregator, target: &mut SessionAggregator, keyless: bool) {
    source
        .visit_canonical_state(|batch| target.import_canonical_state(&batch, usize::from(!keyless)))
        .unwrap();
    source
        .visit_canonical_distinct(0, |batch| target.import_canonical_distinct(0, &batch))
        .unwrap();
    target.finish_canonical_distinct().unwrap();
    target.current_watermark = source.current_watermark;
}

fn render(batch: &RecordBatch) -> Vec<String> {
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| format!("{:?}", ScalarValue::try_from_array(column, row).unwrap()))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn memberships_and_counts_survive_bridging_raw_checkpoints_and_repeated_engine_handoffs() {
    for keyless in [false, true] {
        for counted in [false, true] {
            let mut source = create(keyless, counted);
            source
                .update(&input(
                    keyless,
                    &[0, 100, 2000, 2100],
                    &[Some(10), Some(10), Some(20), None],
                ))
                .unwrap();
            let raw = source.snapshot();
            let mut checkpoint = SessionAggregator::restore(
                1000,
                vec![0, 0],
                vec![
                    if counted {
                        crate::aggregates::COUNT_DISTINCT_WITH_COUNTS
                    } else {
                        7
                    },
                    3,
                ],
                &raw,
            );
            checkpoint.bind_canonical_distinct(0).unwrap();
            let mut target = create(keyless, counted);
            copy(&mut source, &mut target, keyless);
            let mut reexport = create(keyless, counted);
            copy(&mut target, &mut reexport, keyless);
            let suffix = input(
                keyless,
                &[1000, 1100, 1500, 6000],
                &[Some(10), Some(30), Some(20), None],
            );
            for value in [&mut source, &mut checkpoint, &mut target, &mut reexport] {
                value.update(&suffix).unwrap();
            }
            for watermark in [3099, 3100, 7000, i64::MAX] {
                let expected = render(&source.flush(watermark).unwrap());
                for value in [&mut checkpoint, &mut target, &mut reexport] {
                    assert_eq!(expected, render(&value.flush(watermark).unwrap()));
                }
            }
        }
    }
}

#[test]
fn view_exports_are_bounded_retryable_and_imports_reject_duplicates_and_missing_memberships() {
    for keyless in [false, true] {
        let mut source = create(keyless, false);
        source
            .update(&input(
                keyless,
                &vec![0; 2500],
                &(0..2500).map(Some).collect::<Vec<_>>(),
            ))
            .unwrap();
        assert!(source
            .visit_canonical_distinct(0, |_| Err(invalid("receiver stopped")))
            .is_err());
        let mut target = create(keyless, false);
        source
            .visit_canonical_state(|batch| {
                target.import_canonical_state(&batch, usize::from(!keyless))
            })
            .unwrap();
        assert!(target.finish_canonical_distinct().is_err());
        let mut sizes = Vec::new();
        source
            .visit_canonical_distinct(0, |batch| {
                sizes.push(batch.num_rows());
                target.import_canonical_distinct(0, &batch)?;
                assert!(target.import_canonical_distinct(0, &batch).is_err());
                Ok(())
            })
            .unwrap();
        assert_eq!(sizes, vec![1024, 1024, 452]);
        target.finish_canonical_distinct().unwrap();
        assert_eq!(
            render(&source.flush(i64::MAX).unwrap()),
            render(&target.flush(i64::MAX).unwrap())
        );
    }
}

#[test]
fn scalar_cardinality_is_not_interchangeable_with_membership_or_counted_frequencies() {
    for counted in [false, true] {
        let mut source = create(false, counted);
        source
            .update(&input(
                false,
                &[0, 100, 200],
                &[Some(10), Some(10), Some(20)],
            ))
            .unwrap();
        let mut target = create(false, counted);
        source
            .visit_canonical_state(|batch| target.import_canonical_state(&batch, 1))
            .unwrap();
        source
            .visit_canonical_distinct(0, |batch| {
                if counted {
                    let counts = column_i64(&batch, "count");
                    assert!(counts.iter().flatten().any(|count| count == 2));
                }
                let mut columns = batch.columns().to_vec();
                columns[0] = Arc::new(Int64Array::from(vec![9000; batch.num_rows()]));
                assert!(target
                    .import_canonical_distinct(
                        0,
                        &RecordBatch::try_new(batch.schema(), columns).unwrap()
                    )
                    .is_err());
                target.import_canonical_distinct(0, &batch)
            })
            .unwrap();
        target.finish_canonical_distinct().unwrap();
        assert_eq!(
            render(&source.flush(i64::MAX).unwrap()),
            render(&target.flush(i64::MAX).unwrap())
        );
    }
}
