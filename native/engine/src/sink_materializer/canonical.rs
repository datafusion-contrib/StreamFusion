use super::*;
use state::{CanonicalHeader, CanonicalIndex, CanonicalRow};

const CHUNK_ROWS: usize = 1024;

pub(super) fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical sink materializer: {message}"))
}

pub(super) struct ImportGroup {
    key: Vec<u8>,
    header: CanonicalHeader,
    rows: usize,
    indexes: usize,
    _memory: Option<MemoryReservation>,
}

fn field(name: impl Into<String>, data_type: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(Field::new(name, data_type, nullable))
}
fn header_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        field("binary_key", DataType::Binary, false),
        field("linked", DataType::Boolean, false),
        field("highest", DataType::Int64, true),
        field("size", DataType::Int64, false),
        field("written", DataType::Int64, false),
        field("rows", DataType::Int64, false),
        field("indexes", DataType::Int64, false),
    ]))
}
fn row_schema(codec: &RowsCodec) -> SchemaRef {
    let mut fields: Vec<_> = codec
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Arc::new(
                f.as_ref()
                    .clone()
                    .with_name(format!("payload{i}"))
                    .with_nullable(true),
            )
        })
        .collect();
    fields.extend([
        field("binary_key", DataType::Binary, false),
        field("position", DataType::Int64, false),
        field("previous", DataType::Int64, true),
        field("next", DataType::Int64, true),
        field("next_for", DataType::Int64, true),
        field("sampled_at", DataType::Int64, true),
    ]);
    Arc::new(Schema::new(fields))
}
fn index_schema(codec: &RowsCodec) -> SchemaRef {
    let mut fields: Vec<_> = codec
        .match_fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Arc::new(
                f.as_ref()
                    .clone()
                    .with_name(format!("match{i}"))
                    .with_nullable(true),
            )
        })
        .collect();
    fields.extend([
        field("binary_key", DataType::Binary, false),
        field("first", DataType::Int64, false),
        field("last", DataType::Int64, false),
    ]);
    Arc::new(Schema::new(fields))
}
fn check_schema(batch: &RecordBatch, expected: &SchemaRef) -> Result<(), DataFusionError> {
    if batch.num_columns() != expected.fields().len()
        || batch
            .schema()
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
    {
        return Err(invalid("transfer schema changed"));
    }
    for (column, field) in batch.columns().iter().zip(expected.fields()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(invalid("NULL required metadata"));
        }
    }
    Ok(())
}
fn long(batch: &RecordBatch, column: usize, row: usize) -> Option<i64> {
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (!values.is_null(row)).then(|| values.value(row))
}
fn binary(batch: &RecordBatch, column: usize, row: usize) -> &[u8] {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(row)
}
fn keys(key: &[u8], count: usize) -> ArrayRef {
    Arc::new(BinaryArray::from_iter_values((0..count).map(|_| key)))
}
fn header_batch(key: &[u8], header: &CanonicalHeader) -> Result<RecordBatch, DataFusionError> {
    Ok(RecordBatch::try_new(
        header_schema(),
        vec![
            keys(key, 1),
            Arc::new(BooleanArray::from(vec![header.linked])),
            Arc::new(Int64Array::from(vec![header.high])),
            Arc::new(Int64Array::from(vec![header.size])),
            Arc::new(Int64Array::from(vec![header.written])),
            Arc::new(Int64Array::from(vec![header.rows as i64])),
            Arc::new(Int64Array::from(vec![header.indexes as i64])),
        ],
    )?)
}
fn row_batch(
    codec: &RowsCodec,
    key: &[u8],
    rows: &[CanonicalRow],
) -> Result<RecordBatch, DataFusionError> {
    let payloads: Vec<_> = rows.iter().map(|r| r.payload.clone()).collect();
    let mut columns = codec.decode(&payloads);
    columns.extend([
        keys(key, rows.len()),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|r| r.position),
        )),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.previous))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.next))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.next_for))),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.timestamp))),
    ]);
    Ok(RecordBatch::try_new(row_schema(codec), columns)?)
}
fn index_batch(
    codec: &RowsCodec,
    key: &[u8],
    indexes: &[CanonicalIndex],
) -> Result<RecordBatch, DataFusionError> {
    let encoded: Vec<_> = indexes
        .iter()
        .map(|index| codec.encode_identity(&index.identity))
        .collect();
    let parser = codec.identity.parser();
    let mut columns = codec
        .identity
        .convert_rows(encoded.iter().map(|row| parser.parse(row)))?;
    columns.extend([
        keys(key, indexes.len()),
        Arc::new(Int64Array::from_iter_values(
            indexes.iter().map(|i| i.first),
        )),
        Arc::new(Int64Array::from_iter_values(indexes.iter().map(|i| i.last))),
    ]);
    Ok(RecordBatch::try_new(index_schema(codec), columns)?)
}

impl SinkMaterializer {
    fn bind_canonical(&mut self, input: &RecordBatch) -> Result<(), DataFusionError> {
        if input.num_rows() != 0
            || self.canonical_import.is_some()
            || self.groups.iter().next().is_some()
        {
            return Err(invalid(
                "bind requires an empty operator and an empty input-schema batch",
            ));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            store.visit_canonical_keys(|_| {
                Err(invalid("bind requires an empty persistent operator"))
            })?;
        }
        let schema = data_schema(input);
        if self
            .key_columns
            .iter()
            .chain(&self.upsert_columns)
            .any(|&i| i >= schema.fields().len())
        {
            return Err(invalid("input key column missing"));
        }
        if self
            .codec
            .as_ref()
            .is_some_and(|codec| codec.schema != schema)
        {
            return Err(invalid("input schema changed"));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            store.install_schema(&schema, self.ttl_ms)?;
        }
        self.codec = Some(RowsCodec::new(
            schema,
            &self.upsert_columns,
            self.strategy,
            &self.match_precisions,
        ));
        Ok(())
    }

    fn visit_canonical(
        &self,
        section: i32,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if !(0..=2).contains(&section) {
            return Err(invalid("unknown section"));
        }
        if self.canonical_import.is_some() {
            return Err(invalid("group transfer unfinished"));
        }
        let Some(codec) = &self.codec else {
            return Ok(());
        };
        let mut reservation = self.memory.temporary_reservation();
        let mut emit = |batch: RecordBatch| {
            if let Some(memory) = &mut reservation {
                memory.try_resize(batch.get_array_memory_size())?;
            }
            accept(batch)?;
            if let Some(memory) = &mut reservation {
                memory.try_resize(0)?;
            }
            Ok(())
        };
        let mut group = |key: &[u8], memory: Option<&Candidates>| {
            if section == 0 {
                let header = if let Some(values) = memory {
                    values.canonical_header()
                } else {
                    #[cfg(feature = "rocksdb-state")]
                    {
                        self.rocks.as_ref().unwrap().canonical_header(key)?
                    }
                    #[cfg(not(feature = "rocksdb-state"))]
                    {
                        unreachable!()
                    }
                };
                return emit(header_batch(key, &header)?);
            }
            if section == 1 {
                let mut rows = Vec::with_capacity(CHUNK_ROWS);
                let mut visit = |row: CanonicalRow| {
                    if !matches!(self.strategy, Strategy::Legacy) && row.timestamp.is_none() {
                        return Err(invalid("historical candidate timestamp is unknown"));
                    }
                    rows.push(row);
                    if rows.len() == CHUNK_ROWS {
                        emit(row_batch(codec, key, &rows)?)?;
                        rows.clear();
                    }
                    Ok(())
                };
                if let Some(values) = memory {
                    values.visit_canonical_rows(&mut visit)?;
                }
                #[cfg(feature = "rocksdb-state")]
                if memory.is_none() {
                    self.rocks
                        .as_ref()
                        .unwrap()
                        .visit_canonical_rows(key, codec, &mut visit)?;
                }
                if !rows.is_empty() {
                    emit(row_batch(codec, key, &rows)?)?;
                }
            } else {
                let mut indexes = Vec::with_capacity(CHUNK_ROWS);
                let mut visit = |index: CanonicalIndex| {
                    indexes.push(index);
                    if indexes.len() == CHUNK_ROWS {
                        emit(index_batch(codec, key, &indexes)?)?;
                        indexes.clear();
                    }
                    Ok(())
                };
                if let Some(values) = memory {
                    values.visit_canonical_indexes(&mut visit)?;
                }
                #[cfg(feature = "rocksdb-state")]
                if memory.is_none() {
                    self.rocks
                        .as_ref()
                        .unwrap()
                        .visit_canonical_indexes(key, codec, &mut visit)?;
                }
                if !indexes.is_empty() {
                    emit(index_batch(codec, key, &indexes)?)?;
                }
            }
            Ok(())
        };
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            return store.visit_canonical_keys(|key| group(key, None));
        }
        for (key, values) in self.groups.iter() {
            group(&key.0, Some(values))?;
        }
        Ok(())
    }

    fn import_canonical_header(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        check_schema(batch, &header_schema())?;
        if self.codec.is_none() || self.canonical_import.is_some() || batch.num_rows() != 1 {
            return Err(invalid(
                "bind schema, then finish each group before its successor",
            ));
        }
        let key_bytes = binary(batch, 0, 0);
        let header = CanonicalHeader {
            linked: batch
                .column(1)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0),
            high: long(batch, 2, 0),
            size: long(batch, 3, 0).unwrap(),
            written: long(batch, 4, 0).unwrap(),
            rows: usize::try_from(long(batch, 5, 0).unwrap())
                .map_err(|_| invalid("negative row count"))?,
            indexes: usize::try_from(long(batch, 6, 0).unwrap())
                .map_err(|_| invalid("negative index count"))?,
        };
        if header.size < 0
            || !header.linked
                && (header.high.is_some()
                    || header.indexes != 0
                    || header.size as usize != header.rows)
            || matches!(self.strategy, Strategy::Legacy | Strategy::Value) && header.linked
            || matches!(self.strategy, Strategy::Map { .. }) && !header.linked
        {
            return Err(invalid("group representation/counts changed"));
        }
        let mut memory = self.memory.temporary_reservation();
        if let Some(memory) = &mut memory {
            memory.try_resize(key_bytes.len() + std::mem::size_of::<ImportGroup>())?;
        }
        let key = key_bytes.to_vec();
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            store.insert_canonical_header(&key, &header)?;
        }
        #[cfg(feature = "rocksdb-state")]
        let persistent = self.rocks.is_some();
        #[cfg(not(feature = "rocksdb-state"))]
        let persistent = false;
        if !persistent {
            if self.groups.contains(&key) {
                return Err(invalid("group already installed"));
            }
            let values = Candidates::from_canonical_header(&header);
            self.memory
                .record((byte_key_bytes(&key) + values.bytes()) as isize);
            self.groups.insert(ByteKey::from(key.as_slice()), values);
            self.memory.account()?;
        }
        self.canonical_import = Some(ImportGroup {
            key,
            header,
            rows: 0,
            indexes: 0,
            _memory: memory,
        });
        Ok(())
    }

    fn import_canonical_rows(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let codec = self
            .codec
            .as_ref()
            .ok_or_else(|| invalid("input schema not bound"))?;
        check_schema(batch, &row_schema(codec))?;
        if batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("candidate chunk too large"));
        }
        let group = self
            .canonical_import
            .as_mut()
            .ok_or_else(|| invalid("group header missing"))?;
        if group
            .rows
            .checked_add(batch.num_rows())
            .is_none_or(|n| n > group.header.rows)
        {
            return Err(invalid("too many candidate rows"));
        }
        let arity = codec.schema.fields().len();
        let input = RecordBatch::try_new(codec.schema.clone(), batch.columns()[..arity].to_vec())?;
        let payloads = codec.payload.convert_columns(input.columns())?;
        let mut working = self.memory.temporary_reservation();
        if let Some(memory) = &mut working {
            memory.try_resize(payloads.size())?;
        }
        let mut keys = BinaryRowBatchEncoder::new(&input, &self.key_columns, &self.key_precisions);
        for row in 0..batch.num_rows() {
            if binary(batch, arity, row) != group.key || keys.encode(row) != group.key {
                return Err(invalid("candidate key changed"));
            }
            let record = CanonicalRow {
                payload: Arc::from(payloads.row(row).data()),
                position: long(batch, arity + 1, row).unwrap(),
                previous: long(batch, arity + 2, row),
                next: long(batch, arity + 3, row),
                next_for: long(batch, arity + 4, row),
                timestamp: long(batch, arity + 5, row),
            };
            if !matches!(self.strategy, Strategy::Legacy) && record.timestamp.is_none() {
                return Err(invalid("candidate timestamp missing"));
            }
            if !group.header.linked
                && (record.position != group.rows as i64
                    || record.previous.is_some()
                    || record.next.is_some()
                    || record.next_for.is_some())
            {
                return Err(invalid("value candidate position/links changed"));
            }
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &self.rocks {
                store.insert_canonical_row(&group.key, group.header.linked, record)?;
                group.rows += 1;
                continue;
            }
            let values = self
                .groups
                .get_mut(&group.key)
                .ok_or_else(|| invalid("group metadata missing"))?;
            let before = values.bytes();
            values.insert_canonical_row(record, codec)?;
            self.memory
                .record(values.bytes() as isize - before as isize);
            self.memory.account()?;
            group.rows += 1;
        }
        Ok(())
    }

    fn import_canonical_indexes(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let codec = self
            .codec
            .as_ref()
            .ok_or_else(|| invalid("input schema not bound"))?;
        check_schema(batch, &index_schema(codec))?;
        if batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("index chunk too large"));
        }
        let group = self
            .canonical_import
            .as_mut()
            .ok_or_else(|| invalid("group header missing"))?;
        if !group.header.linked
            || group
                .indexes
                .checked_add(batch.num_rows())
                .is_none_or(|n| n > group.header.indexes)
        {
            return Err(invalid("matching-index representation/count changed"));
        }
        let arity = codec.match_fields.len();
        let encoded = codec.identity.convert_columns(&batch.columns()[..arity])?;
        for row in 0..batch.num_rows() {
            if binary(batch, arity, row) != group.key {
                return Err(invalid("matching-index group key changed"));
            }
            let identity = codec.decode_identity(encoded.row(row).data());
            let index = CanonicalIndex {
                identity,
                first: long(batch, arity + 1, row).unwrap(),
                last: long(batch, arity + 2, row).unwrap(),
            };
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &self.rocks {
                store.insert_canonical_index(&group.key, index, codec)?;
                group.indexes += 1;
                continue;
            }
            let values = self
                .groups
                .get_mut(&group.key)
                .ok_or_else(|| invalid("group metadata missing"))?;
            let before = values.bytes();
            values.insert_canonical_index(index, codec)?;
            self.memory
                .record(values.bytes() as isize - before as isize);
            self.memory.account()?;
            group.indexes += 1;
        }
        Ok(())
    }

    fn import_canonical_section(
        &mut self,
        section: i32,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        match section {
            0 => self.import_canonical_header(batch),
            1 => self.import_canonical_rows(batch),
            2 => self.import_canonical_indexes(batch),
            _ => Err(invalid("unknown section")),
        }
    }
    fn finish_canonical_group(&mut self) -> Result<(), DataFusionError> {
        let group = self
            .canonical_import
            .as_ref()
            .ok_or_else(|| invalid("group header missing"))?;
        if group.rows != group.header.rows || group.indexes != group.header.indexes {
            return Err(invalid("group transfer incomplete"));
        }
        self.memory.account()?;
        self.canonical_import = None;
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindSinkMaterializerCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let materializer = unsafe { &mut *(handle as *mut SinkMaterializer) };
        // Arrow release callbacks may enter Java; release input before raising an exception.
        let result = {
            let input = import_record_batch(array, schema);
            materializer.bind_canonical(&input)
        };
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportSinkMaterializerCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    section: jint,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let materializer = unsafe { &*(handle as *const SinkMaterializer) };
        if let Err(error) = materializer.visit_canonical(section, |batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importSinkMaterializerCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    section: jint,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let materializer = unsafe { &mut *(handle as *mut SinkMaterializer) };
        let result = {
            let input = import_record_batch(array, schema);
            materializer.import_canonical_section(section, &input)
        };
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_finishSinkMaterializerCanonicalGroup<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let materializer = unsafe { &mut *(handle as *mut SinkMaterializer) };
        if let Err(error) = materializer.finish_canonical_group() {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}

#[cfg(test)]
mod tests;
