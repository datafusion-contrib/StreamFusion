//! Thin access to the deployed decoder for Criterion; parsing remains in the production module.
use crate::*;

pub struct CsvDecode(crate::csv::CsvDecoder);
impl CsvDecode {
    pub fn new(schema: SchemaRef, skip_errors: bool) -> Self {
        Self(crate::csv::CsvDecoder::new(
            schema,
            CsvOptions::default(),
            skip_errors,
        ))
    }
    pub fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.0.decode(body)
    }
}
