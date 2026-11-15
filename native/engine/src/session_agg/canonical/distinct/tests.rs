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

#[cfg(feature = "rocksdb-state")]
fn persistent(keyless: bool, counted: bool, path: &std::path::Path) -> SessionAggregator {
    let base = create(keyless, counted);
    let store = crate::state::RocksSessionAggStore::create_with_codec(
        crate::state::RocksStoreConfig {
            table_dir: path.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        crate::state::rocks_store::window_memberships::WindowStateCodec::new(&base.aggregates)
            .unwrap(),
        0..=127,
    )
    .unwrap();
    base.with_store(
        store,
        if keyless {
            vec![]
        } else {
            vec![DataType::Int64]
        },
    )
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_sessions_stream_import_and_export_then_bridge_after_engine_switches() {
    for keyless in [false, true] {
        for counted in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut source = create(keyless, counted);
            let mut target = persistent(keyless, counted, &temp.path().join("target"));
            let values = (0..2051).map(Some).collect::<Vec<_>>();
            source
                .update(&input(keyless, &vec![100; values.len()], &values))
                .unwrap();
            source
                .update(&input(keyless, &[2100], &[Some(5000)]))
                .unwrap();
            source
                .visit_canonical_state(|batch| {
                    target.import_canonical_state(&batch, usize::from(!keyless))
                })
                .unwrap();
            assert!(target.update(&input(keyless, &[900], &[Some(7)])).is_err());
            assert!(target.visit_canonical_state(|_| Ok(())).is_err());
            source
                .visit_canonical_distinct(0, |batch| target.import_canonical_distinct(0, &batch))
                .unwrap();
            target.finish_canonical_distinct().unwrap();
            assert!(
                target.sessions.is_empty(),
                "persistent canonical import must retain only disk rows"
            );
            let mut returned = create(keyless, counted);
            copy(&mut target, &mut returned, keyless);
            for (time, value) in [(1100, 7), (1900, 9), (3000, 7)] {
                let batch = input(keyless, &[time], &[Some(value)]);
                for owner in [&mut source, &mut target, &mut returned] {
                    owner.update(&batch).unwrap();
                }
            }
            let expected = render(&source.flush(10000).unwrap());
            assert_eq!(expected, render(&target.flush(10000).unwrap()));
            assert_eq!(expected, render(&returned.flush(10000).unwrap()));
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn previous_snapshot_backed_session_checkpoints_keep_views_and_timer_deadlines() {
    use crate::state::rocks_store::window_memberships::{WindowMembershipState, WindowStateCodec};
    use crate::state::{RocksSessionAggStore, RocksSnapshotStore, RocksStoreConfig};
    for aligned in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut source = create(false, true);
        source
            .update(&input(
                false,
                &[100, 200, 2000],
                &[Some(1), Some(1), Some(5)],
            ))
            .unwrap();
        let parts = source
            .snapshot_partitions(128, &[-1])
            .into_iter()
            .map(|(group, payload)| {
                let mut part = group.to_be_bytes().to_vec();
                part.extend_from_slice(&payload);
                part
            })
            .collect::<Vec<_>>();
        let config = |name: &str| RocksStoreConfig {
            table_dir: temp.path().join(name).to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        };
        let mut old = RocksSnapshotStore::open_merged(config("old"), &[], 0..=127, false).unwrap();
        let saved = temp.path().join("checkpoint");
        let manifest = old
            .checkpoint(&parts, 4567, saved.to_str().unwrap())
            .unwrap();
        drop(old);
        let base = create(false, true);
        let store = RocksSessionAggStore::open_merged_with_codec(
            config("typed"),
            WindowStateCodec::new(&base.aggregates).unwrap(),
            0..=127,
            &[(saved.to_string_lossy().into_owned(), manifest.snapshot_id)],
            aligned,
        )
        .unwrap();
        let mut target = base.with_store(store, vec![DataType::Int64]);
        target.migrate_legacy_snapshot_store().unwrap();
        assert_eq!(target.store_timer_deadline(), 4567);
        let suffix = input(false, &[1100, 1900], &[Some(1), Some(9)]);
        source.update(&suffix).unwrap();
        target.update(&suffix).unwrap();
        assert_eq!(
            render(&source.flush(10000).unwrap()),
            render(&target.flush(10000).unwrap())
        );
        assert!(target
            .store
            .as_ref()
            .unwrap()
            .legacy_snapshot_reader()
            .unwrap()
            .is_none());
    }
}
