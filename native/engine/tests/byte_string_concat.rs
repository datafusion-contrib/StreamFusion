use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use streamfusion::bench::CalcProgram;

fn evaluate(input: ArrayRef, nested: bool) -> RecordBatch {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            input.data_type().clone(),
            true,
        )])),
        vec![input],
    )
    .unwrap();
    let (kinds, payload, children) = if nested {
        (
            vec![6, 6, 0, 3, 3],
            vec![93, 93, 0, 0, 0],
            vec![2, 2, 0, 0, 0],
        )
    } else {
        (vec![6, 0, 3], vec![93, 0, 0], vec![2, 0, 0])
    };
    CalcProgram::new(
        kinds,
        payload,
        children,
        vec![],
        vec![],
        vec![Some("!".into())],
    )
    .run(batch)
}

#[test]
fn compiled_calc_preserves_bytes_across_composed_concatenations() {
    let input = BinaryArray::from(vec![
        Some(&b"skip"[..]),
        Some(&[255]),
        Some(&[65, 228]),
        None,
        Some(&[]),
        Some(&[0]),
        Some(&[195]),
        Some(&[169]),
    ])
    .slice(1, 7);
    let result = evaluate(Arc::new(input), true);
    assert_eq!(
        result.column(0).as_ref(),
        &BinaryArray::from(vec![
            Some(&[255, 33, 33][..]),
            Some(&[65, 228, 33, 33]),
            None,
            Some(&[33, 33]),
            Some(&[0, 33, 33]),
            Some(&[195, 33, 33]),
            Some(&[169, 33, 33]),
        ])
    );
    result.column(0).to_data().validate_full().unwrap();
}

#[test]
fn utf8_consumers_keep_their_existing_result_type_and_nulls() {
    let result = evaluate(
        Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some(""),
            Some("\u{4e2d}"),
        ])),
        true,
    );
    assert_eq!(
        result.column(0).as_ref(),
        &StringArray::from(vec![Some("a!!"), None, Some("!!"), Some("\u{4e2d}!!")])
    );
}

#[test]
fn empty_binary_input_retains_binary_output_schema() {
    let result = evaluate(Arc::new(BinaryArray::from(Vec::<&[u8]>::new())), true);
    assert_eq!(result.num_rows(), 0);
    assert_eq!(result.column(0).data_type(), &DataType::Binary);
}
