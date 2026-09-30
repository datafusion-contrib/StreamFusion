//! In-memory adapters around the production file encoder and decoder.
use arrow::record_batch::RecordBatch;
pub fn encode(batch: &RecordBatch, selected: Option<&[usize]>) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder =
            crate::files::ParquetEncoder::new(&mut bytes, batch.schema(), &[], &[], &[], false);
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
