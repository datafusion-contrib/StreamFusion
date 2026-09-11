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
    _avro_schema: &str,
    _reader_avro_schema: &str,
    _schema_id: i32,
    skip_errors: bool,
    options: &FormatOptions,
) -> MessageDecoder {
    assert_eq!(format, FORMAT_CSV);
    MessageDecoder {
        decoder: Box::new(crate::csv::CsvDecoder::new(
            output_schema,
            options.csv.clone(),
            skip_errors,
        )),
        skip_errors: false,
    }
}
impl Decoder for crate::csv::CsvDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_csv_NativeCsvFormat_createDecoder<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
    skip_parse_errors: jboolean,
    format_options: JString<'local>,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_reader = env.new_string("").expect("empty reader schema");
    create_decoder(
        build_decoder,
        env,
        class,
        FORMAT_CSV,
        schema_array_address,
        schema_address,
        empty_writer,
        empty_reader,
        0,
        skip_parse_errors,
        format_options,
    )
}
