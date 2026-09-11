use super::*;

fn sample_batch() -> RecordBatch {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 6, 3, 9]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 0, 8, 2]));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        vec![a, b],
    )
    .unwrap()
}

fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

// The native sink writes a batch to Parquet; reading it back yields the same rows.
#[test]
fn writes_and_reads_parquet() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let batch = sample_batch();
    let path = std::env::temp_dir().join("streamfusion_parquet_roundtrip.parquet");
    let path = path.to_str().unwrap();
    write_parquet(&batch, path);

    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = 0usize;
    let mut first = Vec::new();
    for read in reader {
        let read = read.unwrap();
        let column = read
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        first.extend_from_slice(column.values());
        rows += read.num_rows();
    }
    assert_eq!(rows, batch.num_rows());
    assert_eq!(first, values(&batch, 0));
}
