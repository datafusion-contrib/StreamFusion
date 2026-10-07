use super::*;

fn json_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

fn bodies(docs: Vec<Option<&[u8]>>) -> RecordBatch {
    let column: ArrayRef = Arc::new(BinaryArray::from(docs));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::Binary,
            true,
        )])),
        vec![column],
    )
    .unwrap()
}

// Each body is one CSV record (no header); CSV decode (format 2) emits one typed row per record.
#[test]
fn csv_decode_emits_one_row_per_record() {
    let body = bodies(vec![Some(b"1,a,1.5"), Some(b"2,b,2.5")]);
    let out = new_decoder(FORMAT_CSV, json_schema(), "", "", 0, false, "").decode(&body);
    assert_eq!(out.num_rows(), 2);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5]);
}

#[test]
fn unicode_numeric_failures_stay_field_local_in_lenient_mode() {
    let body = bodies(vec![
        Some("١٢,valid,é".as_bytes()),
        Some("\u{a0}12,valid,NaNf".as_bytes()),
        Some("-٩٢٢٣٣٧٢٠٣٦٨٥٤٧٧٥٨٠٨,valid,\u{a0}1.5".as_bytes()),
    ]);
    let out = new_decoder(FORMAT_CSV, json_schema(), "", "", 0, true, "").decode(&body);
    assert_eq!(out.num_rows(), 3);
    let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.value(0), 12);
    assert!(ids.is_null(1));
    assert_eq!(ids.value(2), i64::MIN);
    assert_eq!(out.column(2).null_count(), 3);
    assert_eq!(out.column(1).null_count(), 0);
}

#[test]
fn extreme_decimal_exponents_decode_without_materializing_powers_of_ten() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(5, 2),
        true,
    )]));
    let inputs = [
        "0e2147483647",
        "0e-2147483647",
        "0e100000000",
        "1e100000000",
        "1e-100000000",
        "1e2147483647",
        "1e-2147483647",
    ];
    let body = bodies(inputs.iter().map(|s| Some(s.as_bytes())).collect());
    let out = new_decoder(FORMAT_CSV, schema, "", "", 0, true, "").decode(&body);
    let decimals = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        decimals.iter().collect::<Vec<_>>(),
        vec![Some(0), Some(0), Some(0), None, Some(0), None, None]
    );
}
