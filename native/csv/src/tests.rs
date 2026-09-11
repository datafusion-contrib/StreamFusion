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
