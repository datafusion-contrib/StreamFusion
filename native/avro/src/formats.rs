use crate::*;
#[cfg(test)]
pub(crate) fn new_decoder(
    format: i32,
    schema: SchemaRef,
    writer: &str,
    reader: &str,
    schema_id: i32,
    skip_errors: bool,
    options: &str,
) -> MessageDecoder {
    create_message_decoder(
        build_decoder,
        format,
        schema,
        writer,
        reader,
        schema_id,
        skip_errors,
        options,
    )
}

fn build_decoder(
    format: i32,
    output_schema: SchemaRef,
    avro_schema: &str,
    reader_avro_schema: &str,
    schema_id: i32,
    skip_errors: bool,
    _options: &FormatOptions,
) -> MessageDecoder {
    let reader = (!reader_avro_schema.is_empty())
        .then(|| arrow_avro::schema::AvroSchema::new(reader_avro_schema.to_string()));
    let decoder: Box<dyn Decoder> = match format {
        FORMAT_AVRO => Box::new(crate::avro::AvroDecoder::bare(
            avro_schema,
            reader,
            output_schema,
        )),
        FORMAT_AVRO_CONFLUENT => Box::new(crate::avro::AvroDecoder::confluent(
            avro_schema,
            schema_id as u32,
            reader,
            output_schema,
        )),
        FORMAT_DEBEZIUM_AVRO_CONFLUENT => Box::new(AvroCdcDecoder::new(output_schema, reader)),
        _ => panic!("unsupported Avro format {format}"),
    };
    MessageDecoder {
        decoder,
        skip_errors,
    }
}
impl Decoder for crate::avro::AvroDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
    fn register_writer_schema(&mut self, id: u32, schema: &str) {
        self.register_writer_schema(id, schema);
    }
}
impl Decoder for AvroCdcDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
    fn register_writer_schema(&mut self, id: u32, schema: &str) {
        self.register_writer_schema(id, schema);
    }
}
/// Decodes Flink's `debezium-avro-confluent` format: the same `{before, after, op}` Debezium
/// envelope as the JSON dialect, but with Confluent-framed Avro bodies. The composition mirrors
/// Flink's own deserializer — an ordinary registry-Avro decode of the envelope row type (the
/// physical row nullable as both images, plus the op string), then the Debezium op fan-out — so
/// the envelope decode reuses [`crate::avro::AvroDecoder`] whole (id-keyed writer store, schema
/// evolution mid-batch, reconciliation of the image payloads onto the boundary column types) and
/// the fan-out reuses the JSON dialects' emit/gather machinery. A null or empty message is a
/// tombstone, skipped inside the Avro decode; the format has no `ignore-parse-errors`, so every
/// corruption (unknown op, null pre-image on update/delete) fails the job exactly where Flink's
/// deserializer throws.
pub(crate) struct AvroCdcDecoder {
    /// Decodes and reconciles the envelope: `before`/`after` as nullable structs of the (nullable)
    /// physical columns, `op` as Utf8.
    envelope: crate::avro::AvroDecoder,
    /// Output schema: the physical columns (nullable) + trailing `$row_kind$` Int8.
    output: SchemaRef,
    arity: usize,
}

impl AvroCdcDecoder {
    fn new(physical: SchemaRef, reader: Option<arrow_avro::schema::AvroSchema>) -> AvroCdcDecoder {
        let nullable: Fields = physical
            .fields()
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect();
        let image = DataType::Struct(nullable.clone());
        let envelope_target = Arc::new(Schema::new(vec![
            Field::new("before", image.clone(), true),
            Field::new("after", image, true),
            Field::new("op", DataType::Utf8, true),
        ]));
        let mut output_fields: Vec<FieldRef> = nullable.iter().cloned().collect();
        output_fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
        AvroCdcDecoder {
            envelope: crate::avro::AvroDecoder::confluent("", 0, reader, envelope_target)
                .skipping_empty_bodies(),
            output: Arc::new(Schema::new(output_fields)),
            arity: nullable.len(),
        }
    }

    fn register_writer_schema(&mut self, id: u32, schema: &str) {
        self.envelope.register_writer_schema(id, schema);
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let envelope = self.envelope.decode(bodies);
        let before = envelope
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("pre-image struct");
        let after = envelope
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("post-image struct");
        let ops = envelope
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("op string");
        let mut out_rows: Vec<(i8, usize, usize, RowSource)> =
            Vec::with_capacity(envelope.num_rows());
        for row in 0..envelope.num_rows() {
            let op = if ops.is_valid(row) {
                ops.value(row)
            } else {
                panic!("CDC message has no operation field");
            };
            let action = match CdcDialect::Debezium.classify(op) {
                CdcOp::Change(action) => action,
                CdcOp::Skip => continue,
                CdcOp::Unknown => panic!("unknown CDC operation \"{op}\""),
            };
            cdc_emit(
                &action,
                row,
                row,
                CdcShape::BeforeAfter,
                0,
                before,
                after,
                &mut out_rows,
            );
        }
        gather_cdc_batch(&out_rows, before, after, self.arity, &self.output)
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_avro_NativeAvroFormat_createDecoder<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    confluent: jboolean,
    writer_schema: JString<'local>,
    reader_schema: JString<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
    let empty_options = env.new_string("").expect("empty format options");
    create_decoder(
        build_decoder,
        env,
        class,
        if confluent != 0 {
            FORMAT_AVRO_CONFLUENT
        } else {
            FORMAT_AVRO
        },
        schema_array_address,
        schema_address,
        writer_schema,
        reader_schema,
        0,
        0,
        empty_options,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_avro_NativeAvroFormat_createDebeziumDecoder<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    reader_schema: JString<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_options = env.new_string("").expect("empty format options");
    create_decoder(
        build_decoder,
        env,
        class,
        FORMAT_DEBEZIUM_AVRO_CONFLUENT,
        schema_array_address,
        schema_address,
        empty_writer,
        reader_schema,
        0,
        0,
        empty_options,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_avro_NativeAvroFormat_registerWriterSchema<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    handle: jlong,
    schema_id: jint,
    schema: JString<'local>,
) {
    register_avro_schema(env, class, handle, schema_id, schema)
}
