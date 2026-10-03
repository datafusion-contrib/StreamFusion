//! In-memory adapters around the production file encoder and decoder.
use arrow::record_batch::RecordBatch;
pub fn encode(batch: &RecordBatch, selected: Option<&[usize]>) -> Vec<u8> {
    encode_with_int96(batch, selected, false)
}
pub fn encode_with_int96(batch: &RecordBatch, selected: Option<&[usize]>, int96: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let keys = if int96 {
            vec!["timestamp.int96".into()]
        } else {
            vec![]
        };
        let values = if int96 { vec!["true".into()] } else { vec![] };
        let mut encoder = crate::files::ParquetEncoder::new(
            &mut bytes,
            batch.schema(),
            &[],
            &keys,
            &values,
            false,
        );
        match selected {
            Some(rows) => encoder.write_selected(batch, rows),
            None => encoder.write(batch),
        }
        encoder.finish();
    }
    bytes
}
pub fn decode(bytes: &[u8], batch_rows: usize) -> Vec<RecordBatch> {
    crate::reader::decode_for_benchmark(bytes, batch_rows)
}
