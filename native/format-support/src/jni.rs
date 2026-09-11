use crate::*;

pub type DecoderFactory =
    fn(i32, SchemaRef, &str, &str, i32, bool, &FormatOptions) -> MessageDecoder;

#[allow(clippy::too_many_arguments)]
pub fn create_message_decoder(
    factory: DecoderFactory,
    format: i32,
    output_schema: SchemaRef,
    avro_schema: &str,
    reader_avro_schema: &str,
    schema_id: i32,
    skip_errors: bool,
    format_options: &str,
) -> MessageDecoder {
    let options = parse_format_options(format_options);
    if let Some(keyed) = options.keyed.clone() {
        // A keyed table: build the value decoder against the physical schema projected to the
        // value positions (the keyed option lines stripped so the recursion is plain), and the
        // raw key decoder against the key column; the wrapper owns per-record composition.
        let value_options: String = format_options
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with("keyed."))
            .map(|line| format!("{line}\n"))
            .collect();
        let value_schema: SchemaRef = Arc::new(Schema::new(
            keyed
                .value_positions
                .iter()
                .map(|position| output_schema.field(*position).clone())
                .collect::<Vec<_>>(),
        ));
        let value = Box::new(create_message_decoder(
            factory,
            format,
            value_schema,
            avro_schema,
            reader_avro_schema,
            schema_id,
            skip_errors,
            &value_options,
        ));
        let key_schema: SchemaRef = Arc::new(Schema::new(vec![output_schema
            .field(keyed.key_position)
            .clone()]));
        return MessageDecoder {
            decoder: Box::new(KeyedDecoder {
                value,
                key: RawDecoder::new(key_schema, keyed.key_little_endian),
                key_position: keyed.key_position,
                value_positions: keyed.value_positions,
                output: output_schema,
            }),
            skip_errors: false,
        };
    }
    factory(
        format,
        output_schema,
        avro_schema,
        reader_avro_schema,
        schema_id,
        skip_errors,
        &options,
    )
}
/// Creates a format-dispatched message decoder and returns an opaque handle, released with
/// `closeDecoder`. Every format receives the target schema the JVM exports as an empty batch:
/// JSON/CSV/raw decode against it, and the Avro variants reconcile the arrow-avro decode onto it.
/// Formats 1/4 (Confluent/bare Avro) decode via `avroSchema` (registered under `schemaId` for
/// Confluent, synthetic id 0 for bare); a format-1 decoder built with an empty `avroSchema` starts
/// with an empty store — the registry-driven path, where the JVM registers each writer schema by id
/// via `registerAvroSchema` as messages carry it.
pub fn create_decoder<'local>(
    factory: DecoderFactory,
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    reader_avro_schema: JString<'local>,
    schema_id: jint,
    skip_parse_errors: jboolean,
    format_options: JString<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        // Every format decodes against (or, for Avro, reconciles onto) the exported target schema.
        // Only the Avro benchmark counting path passes 0/0 — it never exports the decoded batch.
        let schema = if schema_array_address == 0 {
            Arc::new(Schema::empty())
        } else {
            import_record_batch(schema_array_address, schema_address).schema()
        };
        let avro_schema: String = env
            .get_string(&avro_schema)
            .map(Into::into)
            .unwrap_or_default();
        // Empty unless the planner pushed a projection into an Avro decode: the narrowed reader schema.
        let reader_avro_schema: String = env
            .get_string(&reader_avro_schema)
            .map(Into::into)
            .unwrap_or_default();
        let format_options: String = env
            .get_string(&format_options)
            .map(Into::into)
            .unwrap_or_default();
        into_handle(create_message_decoder(
            factory,
            format,
            schema,
            &avro_schema,
            &reader_avro_schema,
            schema_id,
            skip_parse_errors != 0,
            &format_options,
        ))
    })
}

/// Registers a writer schema under a Confluent schema id on an existing Confluent-Avro decoder. The
/// JVM operator calls this the first time a batch carries an id it hasn't seen: it fetches the schema
/// from the schema registry (as Flink's own `avro-confluent` deserializer does) and feeds it here, so
/// the store grows with the topic's schema evolution instead of being fixed at plan time.
pub fn register_avro_schema<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    schema_id: jint,
    schema: JString<'local>,
) {
    crate::bridge::jni_guard(env, move |env| {
        let decoder = unsafe { &mut *(handle as *mut MessageDecoder) };
        let schema: String = env
            .get_string(&schema)
            .expect("failed to read avro schema")
            .into();
        decoder.register_writer_schema(schema_id as u32, &schema);
    })
}

/// Decodes one body batch into a typed batch, exporting it into the consumer-allocated C structs.
/// A decode failure (bad data outside skip mode) surfaces as a Java `RuntimeException` — the task
/// fails the way Flink's own deserializer failure does — rather than unwinding across the JNI
/// boundary, which would abort the whole process.
pub fn decode_into<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let decoder = unsafe { &*(handle as *mut MessageDecoder) };
    let decoded = catch_unwind(AssertUnwindSafe(|| {
        let bodies = import_record_batch(in_array_address, in_schema_address);
        decoder.decode(&bodies)
    }));
    match decoded {
        Ok(batch) => export_record_batch(batch, out_array_address, out_schema_address),
        Err(panic) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("native decode failed: {}", panic_message(panic)),
            );
        }
    }
}

/// Releases a message decoder handle.
pub fn close_decoder<'local>(env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<MessageDecoder>(handle));
    })
}

thread_local! {
    /// The panic text of this thread's most recent failed C-ABI decode, served by
    /// `decode_last_error`. Thread-local rather than per-decoder because the connector reads it
    /// synchronously on the thread that decoded; the success path never touches it.
    static LAST_DECODE_ERROR: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// This format library's decode behind the cross-DSO driver ABI (`format_abi`): an opaque decoder
/// handle and Arrow C Data addresses, nothing language-specific. A panic is contained and reported
/// as nonzero — its text stashed for `decode_last_error` — and the caller raises the failure on
/// its own JNI surface.
extern "C" fn decode_body_batch(
    handle: i64,
    in_array_address: i64,
    in_schema_address: i64,
    out_array_address: i64,
    out_schema_address: i64,
) -> i32 {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let decoder = unsafe { &*(handle as *mut MessageDecoder) };
        let bodies = import_record_batch(in_array_address, in_schema_address);
        let decoded = decoder.decode(&bodies);
        export_record_batch(decoded, out_array_address, out_schema_address);
    }));
    match outcome {
        Ok(()) => 0,
        Err(payload) => {
            LAST_DECODE_ERROR
                .with(|slot| *slot.borrow_mut() = crate::bridge::panic_message(payload));
            1
        }
    }
}

/// The version-2 error channel: the text of this thread's most recent failed decode. The returned
/// pointer is into a thread-local this library owns; it stays valid until the thread's next failed
/// decode, and the connector copies it out immediately after the nonzero return.
extern "C" fn decode_last_error(_decoder_handle: i64, len_out: *mut i32) -> *const u8 {
    LAST_DECODE_ERROR.with(|slot| {
        let message = slot.borrow();
        unsafe { *len_out = message.len() as i32 };
        message.as_ptr()
    })
}

/// The exported driver init (ADBC's `AdbcDriverInit` pattern): a connector states the ABI version it
/// speaks and passes the matching vtable to fill; this library fills it or refuses with nonzero, and
/// a refusal leaves the caller on the JVM-mediated decode path. Only the requested version's prefix
/// of the vtable is written — an older caller's struct ends there. The connector obtains this
/// function's address through the format's Java facade — by handoff, never by symbol linkage
/// (divergences/25).
pub extern "C" fn streamfusion_format_driver_init(version: i32, driver: *mut FormatDriver) -> i32 {
    if !(FORMAT_DRIVER_VERSION_1..=FORMAT_DRIVER_VERSION_2).contains(&version) || driver.is_null() {
        return 1;
    }
    unsafe {
        (*driver).decode_body_batch = decode_body_batch;
        if version >= FORMAT_DRIVER_VERSION_2 {
            (*driver).decode_last_error = decode_last_error;
        }
    }
    0
}
