use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical join: {message}"))
}

fn throw_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
    }
}

impl<S: JoinStateStore> UpdatingJoiner<S> {
    fn require_canonical_boundary(&self) -> Result<(), DataFusionError> {
        if self.staged_keys() != 0 || self.staging_bytes() != 0 {
            return Err(invalid("pending join bundles must drain before transfer"));
        }
        Ok(())
    }

    fn export_canonical(
        &mut self,
        env: &mut JNIEnv,
        left: bool,
        receiver: &JObject,
    ) -> Result<(), DataFusionError> {
        self.require_canonical_boundary()?;
        let (state, schema, converter) = if left {
            (&mut self.left_state, &self.left_schema, &self.left_payload)
        } else {
            (
                &mut self.right_state,
                &self.right_schema,
                &self.right_payload,
            )
        };
        let parser = converter.parser();
        let mut pending = Vec::with_capacity(CHUNK_ROWS);
        let mut bytes = 0usize;
        state.visit_canonical_rows(&mut |key, payload, meta| {
            let row_bytes = key.len().saturating_add(payload.len()).saturating_add(32);
            if !pending.is_empty() && bytes.saturating_add(row_bytes) > CHUNK_BYTES {
                emit(env, receiver, schema, converter, &pending)?;
                pending.clear();
                bytes = 0;
            }
            pending.push((ByteKey::from(key), parser.parse(payload).owned(), meta));
            bytes = bytes.saturating_add(row_bytes);
            if pending.len() == CHUNK_ROWS || bytes >= CHUNK_BYTES {
                emit(env, receiver, schema, converter, &pending)?;
                pending.clear();
                bytes = 0;
            }
            Ok(())
        })?;
        if !pending.is_empty() {
            emit(env, receiver, schema, converter, &pending)?;
        }
        self.memory
            .record(self.left_state.footprint_delta() + self.right_state.footprint_delta());
        self.memory.account()
    }

    fn import_canonical(
        &mut self,
        left: bool,
        batch: &RecordBatch,
        validate_only: bool,
    ) -> Result<(), DataFusionError> {
        self.require_canonical_boundary()?;
        let (state, schema, converter, columns) = if left {
            (
                &mut self.left_state,
                &self.left_schema,
                &self.left_payload,
                &self.left_keys,
            )
        } else {
            (
                &mut self.right_state,
                &self.right_schema,
                &self.right_payload,
                &self.right_keys,
            )
        };
        let arity = schema.fields().len();
        if batch.num_columns() != arity + 4
            || batch.num_rows() > CHUNK_ROWS
            || batch.schema().fields()[..arity]
                .iter()
                .map(|field| field.data_type())
                .ne(schema.fields().iter().map(|field| field.data_type()))
        {
            return Err(invalid("input schema or transfer batch size changed"));
        }
        let keys = batch
            .column(arity)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid join keys"))?;
        let counts = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid join counts"))?;
        let degrees = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| invalid("invalid join degrees"))?;
        let times = batch
            .column(arity + 3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid join write times"))?;
        let mut projected =
            BinaryRowBatchEncoder::new(batch, columns, &self.key_timestamp_precisions);
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || counts.is_null(row)
                || degrees.is_null(row)
                || times.is_null(row)
                || counts.value(row) <= 0
                || keys.value(row).is_empty()
                || keys.value(row).len() % 8 != 0
                || projected.encode(row) != keys.value(row)
            {
                return Err(invalid("invalid row metadata or physical join key"));
            }
        }
        let payloads = converter.convert_columns(&batch.columns()[..arity])?;
        if validate_only {
            return Ok(());
        }
        state.prepare_canonical_import(keys)?;
        let mut delta = 0;
        for row in 0..batch.num_rows() {
            delta += state.import_canonical_row(
                keys.value(row),
                payloads.row(row).as_ref(),
                RowMeta {
                    count: counts.value(row),
                    num_assoc: degrees.value(row),
                    last_write_ms: times.value(row),
                },
            )?;
        }
        state.end_bundle()?;
        self.memory
            .record(delta + self.left_state.footprint_delta() + self.right_state.footprint_delta());
        self.memory.account()
    }
}

fn emit(
    env: &mut JNIEnv,
    receiver: &JObject,
    schema: &SchemaRef,
    converter: &RowConverter,
    rows: &[(ByteKey, OwnedRow, RowMeta)],
) -> Result<(), DataFusionError> {
    let mut columns = converter
        .convert_rows(rows.iter().map(|(_, row, _)| row.row()))?
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| streamfusion_bridge::timestamp::cast_array(array, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("binary_key", DataType::Binary, false),
        Field::new("count", DataType::Int64, false),
        Field::new("degree", DataType::Int32, false),
        Field::new("written", DataType::Int64, false),
    ]);
    columns.extend([
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|(key, _, _)| key.0.as_ref()),
        )) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|(_, _, meta)| meta.count),
        )),
        Arc::new(Int32Array::from_iter_values(
            rows.iter().map(|(_, _, meta)| meta.num_assoc),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|(_, _, meta)| meta.last_write_ms),
        )),
    ]);
    crate::bridge::emit_record_batch(
        env,
        receiver,
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportUpdatingJoinCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    left: jni::sys::jboolean,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result =
            if persistent == 0 {
                unsafe { &mut *(handle as *mut UpdatingJoiner) }.export_canonical(
                    &mut env,
                    left != 0,
                    &receiver,
                )
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    unsafe { &mut *(handle as *mut UpdatingJoiner<RocksJoinStore>) }
                        .export_canonical(&mut env, left != 0, &receiver)
                }
                #[cfg(not(feature = "rocksdb-state"))]
                {
                    Err(invalid("native RocksDB is unavailable"))
                }
            };
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importUpdatingJoinCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    import_from_java(env, handle, persistent, left, array, schema, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateUpdatingJoinCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jni::sys::jboolean,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    import_from_java(env, handle, persistent, left, array, schema, true);
}

fn import_from_java(
    env: JNIEnv,
    handle: jlong,
    persistent: jni::sys::jboolean,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
    validate_only: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let batch = import_record_batch(array, schema);
        let result =
            if persistent == 0 {
                unsafe { &mut *(handle as *mut UpdatingJoiner) }.import_canonical(
                    left != 0,
                    &batch,
                    validate_only,
                )
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    unsafe { &mut *(handle as *mut UpdatingJoiner<RocksJoinStore>) }
                        .import_canonical(left != 0, &batch, validate_only)
                }
                #[cfg(not(feature = "rocksdb-state"))]
                {
                    Err(invalid("native RocksDB is unavailable"))
                }
            };
        drop(batch);
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

impl crate::state::canonical_memory::CanonicalMemoryState for UpdatingJoiner {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError> {
        target.memory.set(self.memory.state_bytes);
        target.memory.account()?;
        target.left_state = self.left_state.clone();
        target.right_state = self.right_state.clone();
        target.last_sweep_ms = self.last_sweep_ms.clone();
        target.clock_ms = self.clock_ms.clone();
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copyUpdatingJoinCanonicalMemoryState<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    source: jlong,
    target: jlong,
) {
    crate::state::canonical_memory::copy::<UpdatingJoiner>(env, source, target);
}
