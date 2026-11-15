use arrow::array::{Array, BinaryArray, Int32Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use streamfusion::bench::CalcProgram;

#[test]
fn every_first_byte_is_signed_and_slices_nulls_and_empty_batches_match() {
    let bytes = (0..=255)
        .map(|value| vec![value, 255])
        .collect::<Vec<Vec<u8>>>();
    let mut values = vec![Some(&b"unused"[..])];
    values.extend(bytes.iter().map(|value| Some(value.as_slice())));
    values.extend([None, Some(&[][..])]);
    let input = BinaryArray::from(values).slice(1, 258);
    let mut expected = (0..=255)
        .map(|value| Some(value as u8 as i8 as i32))
        .collect::<Vec<_>>();
    expected.extend([None, Some(0)]);
    let batch = batch(input);
    let mut calc = CalcProgram::new(vec![6, 0], vec![67, 0], vec![1, 0], vec![], vec![], vec![]);
    let result = calc.run(batch.clone());
    assert_eq!(result.column(0).as_ref(), &Int32Array::from(expected));
    result.column(0).to_data().validate_full().unwrap();
    let empty = calc.run(batch.slice(0, 0));
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.column(0).data_type(), result.column(0).data_type());
}

#[test]
fn binary_scalars_broadcast_signed_values_and_nulls() {
    let input = batch(BinaryArray::from(vec![&b"ignored"[..]; 3]));
    for (literal, expected) in [
        (vec![1, 255], Some(-1)),
        (vec![-1], None),
        (vec![0], Some(0)),
    ] {
        let result = CalcProgram::new(
            vec![6, 23],
            vec![67, 0],
            vec![1, 0],
            literal,
            vec![],
            vec![],
        )
        .run(input.clone());
        assert_eq!(
            result.column(0).as_ref(),
            &Int32Array::from(vec![expected; 3])
        );
    }
}

fn batch(input: BinaryArray) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            input.data_type().clone(),
            true,
        )])),
        vec![Arc::new(input)],
    )
    .unwrap()
}
