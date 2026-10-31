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
