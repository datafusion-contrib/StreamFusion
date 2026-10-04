use crate::*;

pub struct AvroDecode(crate::avro::AvroDecoder);
impl AvroDecode {
    pub fn new(schema: &str, target: SchemaRef, confluent: bool) -> Self {
        Self(if confluent {
            crate::avro::AvroDecoder::confluent(schema, 1, None, target)
        } else {
            crate::avro::AvroDecoder::bare(schema, None, target)
        })
    }
    pub fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.0.decode(body)
    }
}
