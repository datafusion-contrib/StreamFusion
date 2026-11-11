use super::*;
use crate::state::canonical_context::check_cancelled;

pub(super) const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical temporal join: {message}"))
}

fn append_row_metadata(
    batch: RecordBatch,
    indices: Vec<i64>,
    kinds: Vec<i8>,
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "__canonical_index__",
        DataType::Int64,
        false,
    )));
    fields.push(Arc::new(Field::new(
        "__canonical_kind__",
        DataType::Int8,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(indices)));
    columns.push(Arc::new(Int8Array::from(kinds)));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

impl TemporalJoiner {
    fn canonical_schema(&self, left: bool) -> &SchemaRef {
        if left {
            &self.left_schema
        } else {
            &self.right_schema
        }
    }

    pub(super) fn visit_canonical_rows(
        &self,
        left: bool,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.ensure_canonical_ready()?;
        let schema = self.canonical_schema(left);
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store.visit_canonical_rows(
                left,
                schema,
                CHUNK_ROWS,
                self.memory.temporary_reservation(),
                accept,
            );
        }
        let mut work = self.memory.temporary_reservation();
        let mut rows = Vec::<(&JoinRow, i64, i8)>::with_capacity(CHUNK_ROWS);
        let mut flush = |rows: &mut Vec<(&JoinRow, i64, i8)>| -> Result<(), DataFusionError> {
            if rows.is_empty() {
                return Ok(());
            }
            let retained = rows
                .iter()
                .map(|(row, _, _)| scalar_row_bytes(row))
                .sum::<usize>();
            if let Some(work) = &mut work {
                work.try_resize(retained)?;
            }
            let columns = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(column, field)| {
                    scalars_to_array(
                        rows.iter().map(|(row, _, _)| row[column].clone()).collect(),
                        field.data_type(),
                    )
                })
                .collect();
            let batch = append_row_metadata(
                RecordBatch::try_new(schema.clone(), columns)?,
                rows.iter().map(|(_, index, _)| *index).collect(),
                rows.iter().map(|(_, _, kind)| *kind).collect(),
            )?;
            if let Some(work) = &mut work {
                work.try_resize(retained + batch.get_array_memory_size())?;
            }
            accept(batch)?;
            rows.clear();
            if let Some(work) = &mut work {
                work.resize(0);
            }
            Ok(())
        };
        if left {
            for entries in self.left_state.values() {
                for (&index, entry) in entries {
                    rows.push((&entry.row, index, entry.kind));
                    if rows.len() == CHUNK_ROWS {
                        flush(&mut rows)?;
                    }
                }
            }
        } else {
            for versions in self.right_state.values() {
                for (&timestamp, (row, kind)) in versions {
                    rows.push((row, timestamp, *kind));
                    if rows.len() == CHUNK_ROWS {
                        flush(&mut rows)?;
                    }
                }
            }
        }
        flush(&mut rows)
    }

    pub(super) fn import_canonical_rows(
        &mut self,
        left: bool,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        self.transfer_canonical_rows(left, batch, false)
    }

    pub(super) fn transfer_canonical_rows(
        &mut self,
        left: bool,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let result = self.transfer_rows(left, batch, validating);
        if result.is_err() {
            self.fail_canonical_restore();
        }
        result
    }

    fn transfer_rows(
        &mut self,
        left: bool,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        check_cancelled()?;
        self.check_canonical_restore_pass(validating)?;
        if !self.canonical_import_open {
            return Err(invalid(
                "import requires an operator that has not started execution",
            ));
        }
        let schema = self.canonical_schema(left).clone();
        let arity = schema.fields().len();
        if batch.num_rows() > CHUNK_ROWS
            || batch.num_columns() != arity + 2
            || batch.schema().fields()[..arity]
                .iter()
                .zip(schema.fields())
                .any(|(actual, expected)| actual.data_type() != expected.data_type())
        {
            return Err(invalid("retained row schema or chunk size changed"));
        }
        let indices = batch
            .column(arity)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("missing signed state-map index"))?;
        let kinds = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid("missing original row kind"))?;
        if indices.null_count() != 0
            || kinds.null_count() != 0
            || kinds.values().iter().any(|kind| !(0..=3).contains(kind))
        {
            return Err(invalid("NULL state-map index or invalid row kind"));
        }
        let times = rt_to_millis(batch.column(if left {
            self.left_time
        } else {
            self.right_time
        }));
        if times.null_count() != 0 || !left && times.values() != indices.values() {
            return Err(invalid(
                "NULL row time or build map key disagrees with payload time",
            ));
        }
        let mut work = self.memory.temporary_reservation();
        if let Some(work) = &mut work {
            work.try_grow(batch.get_array_memory_size())?;
        }
        let columns = if left {
            &self.left_keys
        } else {
            &self.right_keys
        };
        let key_arrays = columns
            .iter()
            .map(|column| batch.column(*column))
            .collect::<Vec<_>>();
        let mut unique = HashSet::default();
        for row in 0..batch.num_rows() {
            if !unique.insert((read_key(&key_arrays, row), indices.value(row))) {
                return Err(invalid("duplicate retained row map key"));
            }
        }
        let data = batch.project(&(0..arity).collect::<Vec<_>>())?;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            let keys = store.entry_keys(&data, columns, &self.key_timestamp_precisions);
            store.validate_canonical_row_keys(left, &keys, indices)?;
            if validating {
                return Ok(());
            }
            if left {
                store.import_left(&data, &keys, &times, Some(kinds), indices)?;
            } else {
                store.import_right(&data, &keys, &times, Some(kinds))?;
            }
            return Ok(());
        }
        for (key, index) in &unique {
            let exists = if left {
                self.left_state
                    .get(key)
                    .is_some_and(|rows| rows.contains_key(index))
            } else {
                self.right_state
                    .get(key)
                    .is_some_and(|rows| rows.contains_key(index))
            };
            if exists {
                return Err(invalid("retained row already imported"));
            }
        }
        if validating {
            return Ok(());
        }
        let owned = data
            .columns()
            .iter()
            .map(|column| crate::owned::own_array_data(column.to_data(), true).map(make_array))
            .collect::<Result<Vec<_>, _>>()?;
        let data = RecordBatch::try_new(schema, owned)?;
        let keys = unique
            .iter()
            .map(|(key, index)| (key.clone(), *index))
            .collect::<Vec<_>>();
        let before = self.memory.state_bytes;
        let track = self.memory.tracking();
        let mut delta = 0;
        let key_arrays = columns
            .iter()
            .map(|column| data.column(*column))
            .collect::<Vec<_>>();
        for row in 0..data.num_rows() {
            let key = read_key(&key_arrays, row);
            let values = (0..arity)
                .map(|column| ScalarValue::try_from_array(data.column(column), row))
                .collect::<Result<JoinRow, _>>()?;
            if left {
                let entry = LeftEntry {
                    row: values,
                    time: times.value(row),
                    kind: kinds.value(row),
                };
                if track {
                    delta += left_entry_bytes(&entry);
                    if !self.left_state.contains_key(&key) {
                        delta += group_key_bytes(&key);
                    }
                }
                self.left_state
                    .entry(key)
                    .or_default()
                    .insert(indices.value(row), entry);
            } else {
                if track {
                    delta += right_version_bytes(&values);
                    if !self.right_state.contains_key(&key) {
                        delta += group_key_bytes(&key);
                    }
                }
                self.right_state
                    .entry(key)
                    .or_default()
                    .insert(indices.value(row), (values, kinds.value(row)));
            }
        }
        drop(work);
        self.memory.record(delta as isize);
        if let Err(error) = self.memory.account() {
            for (key, index) in keys {
                if left {
                    if let Some(rows) = self.left_state.get_mut(&key) {
                        rows.remove(&index);
                        if rows.is_empty() {
                            self.left_state.remove(&key);
                        }
                    }
                } else if let Some(rows) = self.right_state.get_mut(&key) {
                    rows.remove(&index);
                    if rows.is_empty() {
                        self.right_state.remove(&key);
                    }
                }
            }
            self.memory.set(before);
            self.memory.account()?;
            return Err(error);
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportTemporalJoinCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const TemporalJoiner) };
        if let Err(error) = joiner.visit_canonical_rows(left != 0, |batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importTemporalJoinCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        let result = joiner.import_canonical_rows(left != 0, &import_record_batch(array, schema));
        if let Err(error) = result {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateTemporalJoinCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, |env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        let result =
            joiner.transfer_canonical_rows(left != 0, &import_record_batch(array, schema), true);
        if let Err(error) = result {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
            }
        }
    });
}

mod metadata;
mod records;
pub(super) use records::MetadataImport;

impl crate::state::canonical_memory::CanonicalMemoryState for TemporalJoiner {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError> {
        self.ensure_canonical_ready()?;
        target.memory.set(self.memory.state_bytes);
        target.memory.account()?;
        target.left_state = self.left_state.clone();
        target.right_state = self.right_state.clone();
        target.metadata = self.metadata.clone();
        target.cleanup_state = self.cleanup_state.clone();
        target.last_sweep_ms = self.last_sweep_ms.clone();
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copyTemporalJoinCanonicalMemoryState<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    source: jlong,
    target: jlong,
) {
    crate::state::canonical_memory::copy::<TemporalJoiner>(env, source, target);
}
