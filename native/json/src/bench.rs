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

/// A JSON format decoder as the connector builds it, by format code (the CDC dialects included).
pub struct FormatDecode(MessageDecoder);

impl FormatDecode {
    pub fn new(format: i32, schema: SchemaRef) -> Self {
        FormatDecode(create_message_decoder(
            build_decoder,
            format,
            schema,
            "",
            "",
            0,
            false,
            "",
        ))
    }

    pub fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        self.0.decode(bodies)
    }
}
