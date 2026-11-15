use super::*;

fn batch(rows: &[(Option<i64>, Option<i64>, i64, i64, i64)]) -> RecordBatch {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "score",
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "tag",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
        (
            "start",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))) as ArrayRef,
        ),
        (
            "end",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.4))) as ArrayRef,
        ),
    ])
    .unwrap();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| field.as_ref().clone().with_nullable(i < 2))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap()
}

fn memory(keyless: bool, limit: i64) -> WindowRanker {
    WindowRanker::new(
        3,
        4,
        if keyless { vec![] } else { vec![0] },
        vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
        limit,
        true,
    )
    .with_memory_budget(64 << 20)
    .unwrap()
}

#[cfg(feature = "rocksdb-state")]
fn ranker(keyless: bool, persistent: bool, limit: i64, path: &std::path::Path) -> WindowRanker {
    let ranker = memory(keyless, limit);
    if !persistent {
        return ranker;
    }
    let schema = batch(&[]).schema();
    let types = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let config = crate::state::RocksStoreConfig {
        table_dir: path.to_str().unwrap().into(),
        max_parallelism: 128,
        options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    };
    ranker
        .with_store(
            crate::state::RocksWindowRankStore::create(config, &types, 0..=127).unwrap(),
            schema,
        )
        .with_key_timestamp_precisions(if keyless {
            vec![i32::MIN]
        } else {
            vec![i32::MIN, -1]
        })
}

fn output(batch: RecordBatch) -> Vec<(Option<i64>, Option<i64>, i64, i64, i64, i64)> {
    let fields = batch
        .columns()
        .iter()
        .map(|c| c.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect::<Vec<_>>();
    let mut rows = (0..batch.num_rows())
        .map(|i| {
            (
                (!fields[0].is_null(i)).then(|| fields[0].value(i)),
                (!fields[1].is_null(i)).then(|| fields[1].value(i)),
                fields[2].value(i),
                fields[3].value(i),
                fields[4].value(i),
                fields[5].value(i),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn pending_windows_and_tie_order_continue_between_memory_and_typed_store() {
    for keyless in [false, true] {
        for from_store in [false, true] {
            for to_store in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let mut source = ranker(keyless, from_store, 4, &directory.path().join("source"));
                let mut target = ranker(keyless, to_store, 4, &directory.path().join("target"));
                let mut rows = Vec::new();
                for (i, key) in [Some(-1), Some(2), None].into_iter().enumerate() {
                    for (j, score) in [Some(10), None, Some(10), Some(30)].into_iter().enumerate() {
                        rows.push((key, score, (i * 10 + j) as i64, 0, 1000));
                        rows.push((key, score, (i * 10 + j + 100) as i64, 1000, 2000));
                    }
                }
                source.push(&batch(&rows)).unwrap();
                source
                    .visit_canonical_rows(|batch| target.import_canonical_rows(&batch))
                    .unwrap();
                let suffix = batch(&[
                    (Some(-1), Some(5), 999, 0, 1000),
                    (None, Some(10), 998, 1000, 2000),
                ]);
                source.push(&suffix).unwrap();
                target.push(&suffix).unwrap();
                assert_eq!(
                    output(source.flush(1000).unwrap()),
                    output(target.flush(1000).unwrap())
                );
                assert_eq!(
                    output(source.flush(2000).unwrap()),
                    output(target.flush(2000).unwrap())
                );
                assert_eq!(source.flush(i64::MAX).unwrap().num_rows(), 0);
                assert_eq!(target.flush(i64::MAX).unwrap().num_rows(), 0);
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn live_visitors_are_bounded_and_receiver_failure_leaves_tie_order_untouched() {
    for persistent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = ranker(false, persistent, 3000, directory.path());
        let input = (0..2500)
            .map(|id| (Some(-1), Some(10), id, 0, 1000))
            .collect::<Vec<_>>();
        source.push(&batch(&input)).unwrap();
        assert!(source
            .visit_canonical_rows(|_| Err(invalid("receiver rejected state")))
            .is_err());
        let mut sizes = Vec::new();
        let mut tags = Vec::new();
        source
            .visit_canonical_rows(|batch| {
                sizes.push(batch.num_rows());
                tags.extend(
                    batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(sizes, vec![1024, 1024, 452]);
        assert_eq!(tags, (0..2500).collect::<Vec<_>>());
        assert_eq!(source.flush(1000).unwrap().num_rows(), 2500);
    }
}

#[test]
fn malformed_or_expired_rows_are_rejected_before_mutating_the_ranker() {
    let mut ranker = memory(false, 3);
    let invalid_bounds = batch(&[(Some(1), Some(10), 1, 1000, 1000)]);
    assert!(ranker.import_canonical_rows(&invalid_bounds).is_err());
    assert!(ranker.groups.is_empty());
    ranker.current_watermark = 1000;
    assert!(ranker
        .import_canonical_rows(&batch(&[(Some(1), Some(10), 1, 0, 1000)]))
        .is_err());
    assert!(ranker.groups.is_empty());
    ranker.current_watermark = i64::MIN;
    let valid = batch(&[(Some(1), Some(10), 1, 0, 1000)]);
    let mut columns = valid.columns().to_vec();
    let mut fields = valid
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(
        streamfusion_bridge::changelog::ROW_KIND_COLUMN,
        DataType::Int8,
        false,
    ));
    columns.push(Arc::new(Int8Array::from(vec![2])));
    let retract = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(ranker.import_canonical_rows(&retract).is_err());
    assert!(ranker.groups.is_empty());
    ranker.import_canonical_rows(&valid).unwrap();
    let columns = vec![
        valid.column(0).clone(),
        Arc::new(StringArray::from(vec!["wrong"])) as ArrayRef,
        valid.column(2).clone(),
        valid.column(3).clone(),
        valid.column(4).clone(),
    ];
    let fields = valid
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if i == 1 {
                Field::new("score", DataType::Utf8, true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    let changed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(ranker.import_canonical_rows(&changed).is_err());
    assert_eq!(ranker.flush(1000).unwrap().num_rows(), 1);
}
