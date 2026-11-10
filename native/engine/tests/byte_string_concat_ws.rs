use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use streamfusion::bench::CalcProgram;

fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, column)| Field::new(format!("c{i}"), column.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn runtime_separators_skip_nulls_but_keep_empty_values_and_raw_bytes() {
    let separators = BinaryArray::from(vec![
        Some(&b"unused"[..]),
        Some(&[255]),
        None,
        Some(&[]),
        Some(&[195]),
        Some(&[169]),
    ])
    .slice(1, 5);
    let values = BinaryArray::from(vec![
        Some(&[65, 228][..]),
        Some(b"x"),
        None,
        Some(&[]),
        None,
    ]);
    // CONCAT_WS(separator, value, NULL, '', '!').
    let result = CalcProgram::new(
        vec![6, 0, 0, 3, 3, 3],
        vec![94, 0, 1, 0, 1, 2],
        vec![5, 0, 0, 0, 0, 0],
        vec![],
        vec![],
        vec![None, Some("".into()), Some("!".into())],
    )
    .run(batch(vec![Arc::new(separators), Arc::new(values)]));
    assert_eq!(
        result.column(0).as_ref(),
        &BinaryArray::from(vec![
            Some(&[65, 228, 255, 255, 33][..]),
            None,
            Some(&[33]),
            Some(&[195, 195, 33]),
            Some(&[169, 33]),
        ])
    );
    result.column(0).to_data().validate_full().unwrap();
}

#[test]
fn separator_only_all_null_values_and_empty_batches_have_correct_validity() {
    let input = batch(vec![Arc::new(BinaryArray::from(vec![
        Some(&[255][..]),
        None,
        Some(&[]),
    ]))]);
    for (kinds, payload, children, strings) in [
        (vec![6, 0], vec![94, 0], vec![1, 0], vec![]),
        (vec![6, 0, 3], vec![94, 0, 0], vec![2, 0, 0], vec![None]),
    ] {
        let mut calc = CalcProgram::new(kinds, payload, children, vec![], vec![], strings);
        assert_eq!(
            calc.run(input.clone()).column(0).as_ref(),
            &BinaryArray::from(vec![Some(&[][..]), None, Some(&[])])
        );
        let empty = calc.run(input.slice(0, 0));
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.column(0).data_type(), input.column(0).data_type());
    }
}

#[test]
fn scalar_binary_separators_broadcast_without_losing_nulls() {
    let input = batch(vec![Arc::new(StringArray::from(vec!["x"; 3]))]);
    for (separator, expected) in [(vec![1, 255], Some(&[b'a', 255][..])), (vec![-1], None)] {
        let result = CalcProgram::new(
            vec![6, 23, 3, 3],
            vec![94, 0, 0, 1],
            vec![3, 0, 0, 0],
            separator,
            vec![],
            vec![Some("a".into()), Some("".into())],
        )
        .run(input.clone());
        assert_eq!(
            result.column(0).as_ref(),
            &BinaryArray::from(vec![expected; 3])
        );
    }
}

#[test]
fn nested_concat_and_utf8_controls_keep_their_physical_types() {
    for column in [
        Arc::new(BinaryArray::from(vec![Some(&b"a"[..]), None, Some(&[])])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("a"), None, Some("")])),
    ] {
        // CONCAT(CONCAT_WS(',', c0, '!'), '?').
        let result = CalcProgram::new(
            vec![6, 6, 3, 0, 3, 3],
            vec![93, 94, 0, 0, 1, 2],
            vec![2, 3, 0, 0, 0, 0],
            vec![],
            vec![],
            vec![Some(",".into()), Some("!".into()), Some("?".into())],
        )
        .run(batch(vec![column.clone()]));
        assert_eq!(result.column(0).data_type(), column.data_type());
        let output = result.column(0);
        let binary;
        let bytes = if let Some(bytes) = output.as_any().downcast_ref::<BinaryArray>() {
            bytes
        } else {
            let strings = output.as_any().downcast_ref::<StringArray>().unwrap();
            binary = BinaryArray::new(
                strings.offsets().clone(),
                strings.values().clone(),
                strings.nulls().cloned(),
            );
            &binary
        };
        assert_eq!(
            bytes,
            &BinaryArray::from(vec![b"a,!?".as_slice(), b"!?", b",!?"])
        );
    }
}
