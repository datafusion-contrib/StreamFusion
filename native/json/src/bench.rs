use crate::*;
pub struct JsonDecode(JsonDecoder);

impl JsonDecode {
    pub fn new(schema: SchemaRef) -> Self {
        JsonDecode(JsonDecoder::new(schema, crate::json::JsonEnv::default()))
    }

    pub fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        self.0.decode(bodies)
    }
}
