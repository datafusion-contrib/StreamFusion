use super::*;

type Input = (i8, Option<i64>, Option<i64>, i64, i64);

fn ranker(variable: bool, ttl: i64) -> RetractableTopNRanker {
    let mut ranker = RetractableTopNRanker::new(
        vec![0],
        vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }],
        0,
        3,
        false,
    )
    .with_key_timestamp_precisions(vec![i32::MIN, -1])
    .with_state_ttl(ttl);
    if variable {
        ranker = ranker.with_rank_end_column(2);
        ranker.enable_first_bound(true);
    }
    ranker.enable_stock_state().unwrap();
    ranker
}

fn frame(input: &[Input]) -> RecordBatch {
    let payload = RecordBatch::try_from_iter(vec![
        (
            "p",
            Arc::new(Int64Array::from(
                input
                    .iter()
                    .map(|r| (r.0 >= 0).then_some(1))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "s",
            Arc::new(Int64Array::from(
                input
                    .iter()
                    .map(|r| (r.0 >= 0).then_some(r.1.unwrap_or(0)))
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "v",
            Arc::new(Int64Array::from(
                input.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ),
    ])
    .unwrap();
    let actual =
        RecordBatch::try_from_iter(vec![("p", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let mut encoder = BinaryRowBatchEncoder::new(&actual, &[0], &[i32::MIN, -1]);
    let key = encoder.encode(0).to_vec();
    let mut columns = payload.columns().to_vec();
    columns.extend([
        Arc::new(BinaryArray::from_iter_values(
            input.iter().map(|_| key.as_slice()),
        )) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(input.iter().map(|r| r.3))),
        Arc::new(Int64Array::from_iter_values(input.iter().map(|r| r.4))),
        Arc::new(Int8Array::from_iter_values(input.iter().map(|r| r.0))),
        Arc::new(Int64Array::from(
            input.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ]);
    let mut fields = payload
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    for (i, array) in columns[3..].iter().enumerate() {
        fields.push(Field::new(
            format!("meta{i}"),
            array.data_type().clone(),
            true,
        ));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn independent_presence_counts_kinds_and_clocks_survive_transfer() {
    let mut state = ranker(true, 1000);
    let input = frame(&[
        (COUNTS, None, None, 0, 300),
        (COUNT, Some(10), None, -3, 300),
        (COUNT, Some(20), None, 0, 300),
        (LIST, Some(30), None, 0, 100),
        (LIST, Some(10), None, 0, 200),
        (1, Some(10), Some(7), 0, 200),
        (BOUND, None, None, -7, 50),
    ]);
    state.transfer_retract(&input, true).unwrap();
    assert_eq!(state.groups.iter().count(), 0);
    state.transfer_retract(&input, false).unwrap();
    let partition = state.groups.iter().next().unwrap().1;
    let retained = partition.retract.as_ref().unwrap();
    assert!(partition.has_state());
    assert_eq!(retained.counts.as_ref().unwrap().entries.len(), 2);
    assert_eq!(retained.lists.len(), 2);
    assert_eq!(retained.bound.unwrap().value, -7);
    let mut emitted = Vec::new();
    visit(true, state.groups.iter(), |rows| {
        for row in rows {
            emitted.push((
                row.tag,
                row.number,
                row.stamp,
                row.payload.map(|row| row.kind()),
            ));
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(emitted.len(), 7);
    assert!(emitted.contains(&(COUNT, -3, 300, None)));
    assert!(emitted.contains(&(COUNT, 0, 300, None)));
    assert!(emitted.contains(&(1, 0, 200, Some(1))));
    assert!(emitted.contains(&(BOUND, -7, 50, None)));
}

#[test]
fn later_frames_validate_without_mutation_and_import_against_prior_headers() {
    let first = frame(&[(COUNTS, None, None, 0, 0), (LIST, Some(10), None, 0, 0)]);
    let second = frame(&[(COUNT, Some(10), None, 9, 0), (2, Some(10), Some(7), 0, 0)]);
    let mut state = ranker(false, 0);
    state.transfer_retract(&first, true).unwrap();
    state.transfer_retract(&second, true).unwrap();
    assert_eq!(state.groups.iter().count(), 0);
    state.transfer_retract(&first, false).unwrap();
    state.transfer_retract(&second, false).unwrap();
    let stored = state
        .groups
        .iter()
        .next()
        .unwrap()
        .1
        .retract
        .as_ref()
        .unwrap();
    assert_eq!(
        *stored
            .counts
            .as_ref()
            .unwrap()
            .entries
            .values()
            .next()
            .unwrap(),
        9
    );
    assert_eq!(stored.lists.values().next().unwrap().rows[0].kind(), 2);
}

#[test]
fn late_bad_metadata_and_duplicate_headers_are_atomic() {
    let cases = [
        vec![(COUNTS, None, None, 0, 0), (8, None, None, 0, 0)],
        vec![(COUNTS, None, None, 0, 0), (COUNT, Some(10), None, 1, 5)],
        vec![(LIST, Some(10), None, 0, 0), (1, Some(10), Some(7), 0, 5)],
        vec![(COUNTS, None, None, 0, 0), (COUNTS, None, None, 0, 0)],
        vec![(LIST, Some(10), None, 0, 0), (LIST, Some(10), None, 0, 0)],
        vec![(COUNTS, None, None, 0, 0), (BOUND, None, None, 3, 0)],
        vec![(COUNT, Some(10), None, 1, 0)],
        vec![(1, Some(10), Some(7), 0, 0)],
    ];
    for case in cases {
        let mut state = ranker(false, 0);
        assert!(state.transfer_retract(&frame(&case), false).is_err());
        assert_eq!(state.groups.iter().count(), 0, "{case:?}");
    }
    let mut state = ranker(false, 0);
    let header = frame(&[(COUNTS, None, None, 0, 0)]);
    state.transfer_retract(&header, false).unwrap();
    assert!(state
        .transfer_retract(
            &frame(&[(LIST, Some(10), None, 0, 0), (COUNTS, None, None, 0, 0)]),
            false
        )
        .is_err());
    assert!(state
        .groups
        .iter()
        .next()
        .unwrap()
        .1
        .retract
        .as_ref()
        .unwrap()
        .lists
        .is_empty());
}

#[test]
fn count_only_keys_and_zero_age_projection_do_not_mutate_native_clocks() {
    let mut state = ranker(false, 1000);
    state
        .transfer_retract(&frame(&[(COUNTS, None, None, 0, 5000)]), false)
        .unwrap();
    let partition = state.groups.iter().next().unwrap().1;
    assert!(partition.has_state());
    let mut sizes = Vec::new();
    visit(false, state.groups.iter(), |rows| {
        sizes.push(rows.len());
        assert_eq!(rows[0].tag, COUNTS);
        assert_eq!(rows[0].stamp, 0);
        Ok(())
    })
    .unwrap();
    assert_eq!(sizes, vec![1]);
    assert_eq!(
        partition
            .retract
            .as_ref()
            .unwrap()
            .counts
            .as_ref()
            .unwrap()
            .written_at,
        5000
    );
}

#[test]
fn payload_partition_and_sort_disagreements_fail_before_inserting_headers() {
    let good = frame(&[(LIST, Some(10), None, 0, 0), (0, Some(10), Some(7), 0, 0)]);
    for column in [0, 1] {
        let mut columns = good.columns().to_vec();
        columns[column] = Arc::new(Int64Array::from(vec![None, Some(99)]));
        let bad = RecordBatch::try_new(good.schema(), columns).unwrap();
        let mut state = ranker(false, 0);
        assert!(state.transfer_retract(&bad, false).is_err());
        assert_eq!(state.groups.iter().count(), 0);
    }
}
