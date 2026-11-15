use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use std::sync::Arc;
use streamfusion::bench::CalcProgram;

#[test]
fn base64_encodes_all_octets_and_preserves_slices_nulls_and_empty_batches() {
    let octets = (0..=255).collect::<Vec<u8>>();
    let input = BinaryArray::from(vec![
        Some(&b"unused"[..]),
        Some(&octets),
        Some(&[255]),
        Some(&[65, 228]),
        None,
        Some(&[]),
        Some(&[0]),
    ])
    .slice(1, 6);
    let expected = StringArray::from(
        input
            .iter()
            .map(|value| value.map(|v| base64::engine::general_purpose::STANDARD.encode(v)))
            .collect::<Vec<_>>(),
    );
    let batch = batch(Arc::new(input));
    let mut calc = program();
    let result = calc.run(batch.clone());
    assert_eq!(result.column(0).as_ref(), &expected);
    result.column(0).to_data().validate_full().unwrap();
    assert_eq!(calc.run(batch.slice(0, 0)).num_rows(), 0);
}

#[test]
fn base64_keeps_the_existing_utf8_results() {
    let values = [Some(""), None, Some("abc"), Some("\u{4e2d}\0")];
    let expected = StringArray::from(
        values
            .map(|value| value.map(|v| base64::engine::general_purpose::STANDARD.encode(v)))
            .to_vec(),
    );
    for input in [
        Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
        Arc::new(BinaryArray::from(
            values.map(|v| v.map(str::as_bytes)).to_vec(),
        )),
    ] {
        assert_eq!(program().run(batch(input)).column(0).as_ref(), &expected);
    }
}

fn batch(input: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            input.data_type().clone(),
            true,
        )])),
        vec![input],
    )
    .unwrap()
}

fn program() -> CalcProgram {
    CalcProgram::new(vec![6, 0], vec![107, 0], vec![1, 0], vec![], vec![], vec![])
}
