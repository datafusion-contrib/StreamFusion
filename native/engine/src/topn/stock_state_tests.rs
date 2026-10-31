use super::*;

fn ranker(limit: i64) -> TopNRanker {
    TopNRanker::new(
        vec![0],
        vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
        limit,
        true,
        false,
    )
}

#[test]
fn projected_rank_keeps_hidden_overflow_without_emitting_it_in_either_bundle_mode() {
    for mini in [false, true] {
        let mut ranker = ranker(3);
        ranker.net_diff = mini;
        let input = |scores: Vec<i64>| {
            RecordBatch::try_from_iter(vec![
                (
                    "k",
                    Arc::new(Int64Array::from(vec![1; scores.len()])) as ArrayRef,
                ),
                ("v", Arc::new(Int64Array::from(scores))),
            ])
            .unwrap()
        };
        ranker.push(&input(vec![10, 20, 30]), 1000).unwrap();
        if mini {
            assert_eq!(ranker.flush_net_diff().num_rows(), 3);
        }
        ranker.push(&input(vec![15]), 1500).unwrap();
        assert_eq!(ranker.groups.iter().next().unwrap().1.len(), 4);
        ranker.push(&input(vec![12]), 1600).unwrap();
        let partition = ranker.groups.iter().next().unwrap().1;
        assert_eq!(partition.len(), 4);
        let payload = ranker
            .converters
            .as_ref()
            .unwrap()
            .payload
            .convert_rows(partition.iter().map(|row| row.payload.row()))
            .unwrap();
        let scores = payload[1].as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(scores.values().as_ref(), &[10, 12, 15, 20]);
        if mini {
            let result = ranker.flush_net_diff();
            let ranks = result
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(ranks.values().iter().all(|rank| *rank <= 3));
        }
    }
}

#[test]
fn fast_top1_replacements_emit_updates_even_when_rank_is_not_projected() {
    for mini in [false, true] {
        for before in [false, true] {
            let mut ranker = ranker(1);
            ranker.output_rank_number = false;
            ranker.generate_update_before = before;
            ranker.net_diff = mini;
            let input = |score| {
                RecordBatch::try_from_iter(vec![
                    ("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                    ("v", Arc::new(Int64Array::from(vec![score]))),
                ])
                .unwrap()
            };
            ranker.push(&input(10), 1000).unwrap();
            if mini {
                ranker.flush_net_diff();
            }
            let immediate = ranker.push(&input(5), 1100).unwrap();
            let result = if mini {
                ranker.flush_net_diff()
            } else {
                immediate
            };
            let kinds = result
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            assert_eq!(
                kinds.values().as_ref(),
                if before { &[1, 2][..] } else { &[2][..] }
            );
        }
    }
}

fn updating_ranker(projected: bool) -> UpdatableTopNRanker {
    UpdatableTopNRanker::new(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![SortColumn {
            index: 2,
            ascending: true,
            nulls_first: true,
        }],
        3,
        projected,
        true,
    )
}

fn updating_batch(rows: &[(i64, i64, i8)]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from(vec![1; rows.len()])) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
        ),
        (
            "score",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|row| row.2))),
        ),
    ])
    .unwrap()
}

#[test]
fn updating_projected_rank_retains_the_overflow_group_and_continues_after_restore() {
    let mut ranker = updating_ranker(true);
    ranker
        .push(&updating_batch(&[(3, 10, 0), (4, 20, 0), (5, 30, 0)]), 0)
        .unwrap();
    ranker.push(&updating_batch(&[(2, 15, 0)]), 0).unwrap();
    assert_eq!(ranker.groups.iter().next().unwrap().1.len(), 4);
    let snapshots = ranker
        .snapshot_partitions(128)
        .into_values()
        .collect::<Vec<_>>();
    let mut restored = UpdatableTopNRanker::restore_partitions(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![SortColumn {
            index: 2,
            ascending: true,
            nulls_first: true,
        }],
        3,
        true,
        true,
        &snapshots,
        0,
    );
    let input = updating_batch(&[(5, 7, 2)]);
    assert_eq!(
        write_ipc(&ranker.push(&input, 0).unwrap()),
        write_ipc(&restored.push(&input, 0).unwrap())
    );
    assert_eq!(restored.groups.iter().next().unwrap().1[0].stored_kind, 2);
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn updating_raw_codec_accepts_old_rows_and_preserves_new_row_kinds_with_bounds() {
    use crate::state::RocksStateCodec;
    let mut ranker = updating_ranker(true);
    ranker
        .push(&updating_batch(&[(3, 10, 0), (4, 20, 0)]), 0)
        .unwrap();
    let codec = UpdatableTopNStateCodec::new(ranker.converters.as_ref().unwrap());
    let key = ranker.groups.iter().next().unwrap().0 .0.to_vec();
    let partition = ranker.groups.get_mut(&key).unwrap();
    let mut old = Vec::new();
    codec.raw_write(partition, &mut old);
    assert_eq!(codec.value_bytes(partition), old.len());
    assert!(codec.from_raw(&old).iter().all(|row| row.stored_kind == 0));
    partition[1].stored_kind = 2;
    partition.first_rank_end = Some(FirstRankEnd {
        value: 3,
        written_at: 1234,
    });
    let mut new = Vec::new();
    codec.raw_write(partition, &mut new);
    assert_eq!(codec.value_bytes(partition), new.len());
    assert_eq!(&new[..old.len()], &old);
    let decoded = codec.from_raw(&new);
    assert_eq!(
        decoded
            .iter()
            .map(|row| row.stored_kind)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(decoded.first_rank_end.unwrap().written_at, 1234);
}
