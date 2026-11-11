use arrow::array::{Array, ArrayRef, BinaryArray, NullArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sha2::Digest;
use std::sync::Arc;
use streamfusion::bench::CalcProgram;

fn check_hash<D: Digest>(operation: i64) {
    let values = vec![
        Some(&b"skip"[..]),
        Some(&[255]),
        Some(&[65, 228]),
        None,
        Some(&[]),
        Some(&[0]),
        Some(&[195]),
        Some(&[169]),
    ];
    let bytes = BinaryArray::from(values).slice(1, 7);
    let expected = StringArray::from(
        bytes
            .iter()
            .map(|value| {
                value.map(|v| {
                    D::digest(v)
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                })
            })
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            bytes.data_type().clone(),
            true,
        )])),
        vec![Arc::new(bytes)],
    )
    .unwrap();
    let mut calc = CalcProgram::new(
        vec![6, 0],
        vec![operation, 0],
        vec![1, 0],
        vec![],
        vec![],
        vec![],
    );
    let output = calc.run(batch.clone());
    assert_eq!(output.column(0).as_ref(), &expected);
    output.column(0).to_data().validate_full().unwrap();
    assert_eq!(calc.run(batch.slice(0, 0)).num_rows(), 0);

    for column in [
        Arc::new(BinaryArray::from(vec![Some(&b"abc"[..]), None, Some(&[])])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("abc"), None, Some("")])),
    ] {
        let expected = StringArray::from(
            [Some(b"abc".as_slice()), None, Some(b"")]
                .map(|value| {
                    value.map(|v| {
                        D::digest(v)
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>()
                    })
                })
                .to_vec(),
        );
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                column.data_type().clone(),
                true,
            )])),
            vec![column],
        )
        .unwrap();
        let mut calc = CalcProgram::new(
            vec![6, 0],
            vec![operation, 0],
            vec![1, 0],
            vec![],
            vec![],
            vec![],
        );
        assert_eq!(calc.run(input).column(0).as_ref(), &expected);
    }

    let nulls = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Null, true)])),
        vec![Arc::new(NullArray::new(3))],
    )
    .unwrap();
    let mut calc = CalcProgram::new(
        vec![6, 0],
        vec![operation, 0],
        vec![1, 0],
        vec![],
        vec![],
        vec![],
    );
    let result = calc.run(nulls.clone());
    assert_eq!(result.column(0).data_type(), &DataType::Utf8);
    assert_eq!(result.column(0).null_count(), 3);
    assert_eq!(calc.run(nulls.slice(0, 0)).schema(), result.schema());
}

#[test]
fn md5_hashes_binary_and_utf8_with_identical_null_and_slice_rules() {
    check_hash::<md5::Md5>(95);
}

#[test]
fn sha1_hashes_binary_and_utf8_with_identical_null_and_slice_rules() {
    check_hash::<sha1::Sha1>(143);
}

#[test]
fn sha224_hashes_binary_and_utf8_with_identical_null_and_slice_rules() {
    check_hash::<sha2::Sha224>(96);
}

#[test]
fn sha256_hashes_binary_and_utf8_with_identical_null_and_slice_rules() {
    check_hash::<sha2::Sha256>(97);
}

#[test]
fn sha384_hashes_binary_and_utf8_with_identical_null_and_slice_rules() {
    check_hash::<sha2::Sha384>(98);
}

#[test]
fn sha512_hashes_binary_and_utf8_with_identical_null_and_slice_rules() {
    check_hash::<sha2::Sha512>(99);
}
