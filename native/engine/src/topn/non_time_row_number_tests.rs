use super::*;

fn batch(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![("v", Arc::new(Int64Array::from(values)) as ArrayRef)]).unwrap()
}

fn ranker() -> TopNRanker {
    let mut ranker = TopNRanker::new(
        vec![],
        vec![SortColumn {
            index: 0,
            ascending: true,
            nulls_first: true,
        }],
        i64::MAX,
        true,
        false,
    );
    ranker.enable_non_time_row_number();
    ranker
}

fn changes(batch: &RecordBatch) -> Vec<(i8, Option<i64>, i64)> {
    if batch.num_rows() == 0 {
        return vec![];
    }
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ranks = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let kinds = row_kind_column(batch).unwrap();
    (0..batch.num_rows())
        .map(|i| {
            (
                kinds.value(i),
                (!values.is_null(i)).then(|| values.value(i)),
                ranks.value(i),
            )
        })
        .collect()
}

#[test]
fn inserted_row_keeps_insert_kind_and_existing_rows_keep_identity() {
    let mut ranker = ranker();
    assert_eq!(
        changes(
            &ranker
                .push(&batch(vec![Some(30), Some(10), Some(10)]), 0)
                .unwrap()
        ),
        vec![
            (0, Some(30), 1),
            (0, Some(10), 1),
            (1, Some(30), 1),
            (2, Some(30), 2),
            (0, Some(10), 2),
            (1, Some(30), 2),
            (2, Some(30), 3)
        ]
    );
    assert_eq!(
        changes(&ranker.push(&batch(vec![None]), 0).unwrap()),
        vec![
            (0, None, 1),
            (1, Some(10), 1),
            (2, Some(10), 2),
            (1, Some(10), 2),
            (2, Some(10), 3),
            (1, Some(30), 3),
            (2, Some(30), 4)
        ]
    );
}

#[test]
fn all_rows_survive_raw_restore_and_continue_ranking() {
    let mut original = ranker();
    original
        .push(&batch((0..2048).map(Some).collect()), 0)
        .unwrap();
    assert_eq!(
        original
            .groups
            .iter()
            .map(|(_, rows)| rows.len())
            .sum::<usize>(),
        2048
    );
    let snapshots: Vec<_> = original.snapshot_partitions(128).into_values().collect();
    let mut restored = TopNRanker::restore_partitions(
        vec![],
        vec![],
        original.sort_columns.clone(),
        i64::MAX,
        true,
        false,
        &snapshots,
        0,
    );
    restored.enable_non_time_row_number();
    let suffix = batch(vec![Some(2048), Some(-1), None, Some(0)]);
    assert_eq!(
        changes(&original.push(&suffix, 0).unwrap()),
        changes(&restored.push(&suffix, 0).unwrap())
    );
    assert_eq!(
        restored
            .groups
            .iter()
            .map(|(_, rows)| rows.len())
            .sum::<usize>(),
        2052
    );
}

#[test]
fn empty_input_and_memory_budget_use_the_existing_operator_contract() {
    let mut empty = ranker();
    assert_eq!(empty.push(&batch(vec![]), 0).unwrap().num_rows(), 0);
    let mut limited = ranker().with_memory_budget(64).unwrap();
    assert!(limited.push(&batch(vec![Some(1), Some(2)]), 0).is_err());
}

#[test]
fn floating_groups_follow_linear_host_comparison_across_restores() {
    use std::cmp::Ordering;
    for single in [false, true] {
        for nulls_first in [false, true] {
            for reverse in [false, true] {
                let mut values: Vec<Option<f64>> = vec![
                    None,
                    Some(3.0),
                    Some(-0.0),
                    Some(f64::from_bits(0xfff8000000001234)),
                    Some(0.0),
                    Some(-2.0),
                    Some(f64::NAN),
                    Some(f64::NEG_INFINITY),
                    Some(1.0),
                    Some(f64::INFINITY),
                    None,
                    Some(f64::from_bits(0x7ff8000000004321)),
                ];
                values.extend(values.clone());
                if reverse {
                    values.reverse();
                }
                let input = |begin: usize, end: usize| {
                    let value: ArrayRef = if single {
                        Arc::new(Float32Array::from_iter(
                            values[begin..end].iter().map(|v| v.map(|v| v as f32)),
                        ))
                    } else {
                        Arc::new(Float64Array::from(values[begin..end].to_vec()))
                    };
                    RecordBatch::try_from_iter(vec![
                        ("v", value),
                        (
                            "id",
                            Arc::new(Int64Array::from_iter_values((begin..end).map(|i| i as i64)))
                                as ArrayRef,
                        ),
                    ])
                    .unwrap()
                };
                let compare = |a: Option<f64>, b: Option<f64>| match (a, b) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => {
                        if nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (Some(_), None) => {
                        if nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                };
                let mut groups: Vec<(Option<f64>, Vec<i64>)> = vec![];
                let mut expected = vec![];
                for (id, &value) in values.iter().enumerate() {
                    let group = groups
                        .iter()
                        .position(|(key, _)| compare(*key, value) != Ordering::Less)
                        .unwrap_or(groups.len());
                    let equal =
                        group < groups.len() && compare(groups[group].0, value) == Ordering::Equal;
                    let pos = groups
                        .iter()
                        .take(group)
                        .map(|(_, ids)| ids.len())
                        .sum::<usize>()
                        + if equal { groups[group].1.len() } else { 0 };
                    let before: Vec<_> = groups
                        .iter()
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                    expected.push((0, id as i64, pos as i64 + 1));
                    for (index, &old) in before.iter().enumerate().skip(pos) {
                        expected.push((1, old, index as i64 + 1));
                        expected.push((2, old, index as i64 + 2));
                    }
                    if equal {
                        groups[group].0 = value;
                        groups[group].1.push(id as i64);
                    } else {
                        groups.insert(group, (value, vec![id as i64]));
                    }
                }
                let sort = vec![SortColumn {
                    index: 0,
                    ascending: true,
                    nulls_first,
                }];
                let mut state = TopNRanker::new(vec![], sort.clone(), i64::MAX, true, false);
                state.enable_non_time_row_number();
                let mut actual = vec![];
                for (start, end) in [(0, 7), (7, 17), (17, values.len())] {
                    let out = state.push(&input(start, end), 0).unwrap();
                    let ids = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                    let ranks = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                    let kinds = row_kind_column(&out).unwrap();
                    actual.extend(
                        (0..out.num_rows()).map(|i| (kinds.value(i), ids.value(i), ranks.value(i))),
                    );
                    let snapshots: Vec<_> = state.snapshot_partitions(128).into_values().collect();
                    state = TopNRanker::restore_partitions(
                        vec![],
                        vec![],
                        sort.clone(),
                        i64::MAX,
                        true,
                        false,
                        &snapshots,
                        0,
                    );
                    state.enable_non_time_row_number();
                }
                assert_eq!(
                    actual, expected,
                    "single={single} nulls_first={nulls_first} reverse={reverse}"
                );
            }
        }
    }
}

#[test]
fn signed_updates_remove_first_full_identity_and_shift_each_survivor() {
    fn signed(values: Vec<Option<i64>>, kinds: Vec<i8>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("v", Arc::new(Int64Array::from(values)) as ArrayRef),
            (
                ROW_KIND_COLUMN,
                Arc::new(Int8Array::from(kinds)) as ArrayRef,
            ),
        ])
        .unwrap()
    }
    let mut original = ranker();
    original
        .push(&batch(vec![Some(30), Some(10), Some(10)]), 0)
        .unwrap();
    let snapshots: Vec<_> = original.snapshot_partitions(128).into_values().collect();
    let mut restored = TopNRanker::restore_partitions(
        vec![],
        vec![],
        original.sort_columns.clone(),
        i64::MAX,
        true,
        false,
        &snapshots,
        0,
    );
    restored.enable_non_time_row_number();
    let suffix = signed(
        vec![Some(10), Some(99), Some(30), Some(20), None],
        vec![3, 3, 1, 2, 0],
    );
    let expected = vec![
        (3, Some(10), 1),
        (1, Some(10), 2),
        (2, Some(10), 1),
        (1, Some(30), 3),
        (2, Some(30), 2),
        (3, Some(30), 2),
        (2, Some(20), 2),
        (0, None, 1),
        (1, Some(10), 1),
        (2, Some(10), 2),
        (1, Some(20), 2),
        (2, Some(20), 3),
    ];
    assert_eq!(changes(&original.push(&suffix, 0).unwrap()), expected);
    assert_eq!(changes(&restored.push(&suffix, 0).unwrap()), expected);
}

#[test]
fn deleting_the_latest_nan_keeps_the_groups_last_assigned_comparison_key() {
    let mut ranker = ranker();
    let input = |values, kinds| {
        RecordBatch::try_from_iter(vec![
            ("v", Arc::new(Float64Array::from(values)) as ArrayRef),
            (
                ROW_KIND_COLUMN,
                Arc::new(Int8Array::from(kinds)) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    ranker
        .push(&input(vec![1.0, f64::NAN], vec![0, 0]), 0)
        .unwrap();
    let snapshots: Vec<_> = ranker.snapshot_partitions(128).into_values().collect();
    let mut restored = TopNRanker::restore_partitions(
        vec![],
        vec![],
        ranker.sort_columns.clone(),
        i64::MAX,
        true,
        false,
        &snapshots,
        0,
    );
    restored.enable_non_time_row_number();
    for operator in [&mut ranker, &mut restored] {
        let output = operator
            .push(&input(vec![f64::NAN, 0.0], vec![3, 0]), 0)
            .unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(row_kind_column(&output).unwrap().values(), &[3, 0]);
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 2]
        );
    }
}

#[test]
fn retractions_probe_the_first_greater_nan_group_for_the_full_payload() {
    let mut operator = ranker();
    let input = |values, kinds| {
        RecordBatch::try_from_iter(vec![
            ("v", Arc::new(Float64Array::from(values)) as ArrayRef),
            (
                ROW_KIND_COLUMN,
                Arc::new(Int8Array::from(kinds)) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    operator
        .push(&input(vec![0.0, f64::NAN, 2.0], vec![0, 0, 0]), 0)
        .unwrap();
    let output = operator.push(&input(vec![0.0], vec![3]), 0).unwrap();
    assert_eq!(output.num_rows(), 5);
    assert_eq!(row_kind_column(&output).unwrap().values(), &[3, 1, 2, 1, 2]);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2, 1, 3, 2]
    );
}
