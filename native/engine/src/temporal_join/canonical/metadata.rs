use super::*;
use crate::temporal_join::stock_meta::StockTemporalMeta;

fn metadata_batch(
    mut columns: Vec<ArrayRef>,
    types: &[DataType],
    values: &[(&StockTemporalMeta, Option<i64>)],
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = key_fields(types);
    for (name, column) in [
        (
            "__next_index__",
            Arc::new(Int64Array::from_iter(
                values.iter().map(|(value, _)| value.next_index),
            )) as ArrayRef,
        ),
        (
            "__registered__",
            Arc::new(Int64Array::from_iter(
                values.iter().map(|(value, _)| value.registered),
            )),
        ),
        (
            "__cleanup__",
            Arc::new(Int64Array::from_iter(
                values.iter().map(|(_, cleanup)| *cleanup),
            )),
        ),
        (
            "__callbacks__",
            Arc::new(ListArray::from_iter_primitive::<
                arrow::array::types::Int64Type,
                _,
                _,
            >(values.iter().map(|(value, _)| {
                Some(value.callbacks.iter().copied().map(Some))
            }))),
        ),
    ] {
        fields.push(Field::new(
            name,
            column.data_type().clone(),
            name != "__callbacks__",
        ));
        columns.push(column);
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn optional(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Option<i64>, DataFusionError> {
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("invalid metadata value type"))?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

impl TemporalJoiner {
    pub(in crate::temporal_join) fn visit_canonical_metadata(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let types = self
            .left_keys
            .iter()
            .map(|column| self.left_schema.field(*column).data_type().clone())
            .collect::<Vec<_>>();
        let mut work = self.memory.temporary_reservation();
        let empty = StockTemporalMeta::default();
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store.visit_canonical_metadata(CHUNK_ROWS, |rows| {
                // Reserve raw values and callback-tree storage before decoding captured metadata.
                let retained = rows
                    .iter()
                    .map(|(key, value, _)| {
                        key.0.len() * 2 + value.as_ref().map_or(0, |value| value.len() * 6) + 64
                    })
                    .sum::<usize>();
                if let Some(work) = &mut work {
                    work.try_resize(retained)?;
                }
                let metadata = rows
                    .iter()
                    .map(|(_, value, _)| {
                        value.as_ref().map_or_else(
                            || Ok(StockTemporalMeta::default()),
                            |value| {
                                StockTemporalMeta::decode(value).map_err(DataFusionError::Execution)
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let keys = rows.iter().map(|(key, _, _)| key).collect::<Vec<_>>();
                let values = metadata
                    .iter()
                    .zip(&rows)
                    .map(|(meta, (_, _, deadline))| (meta, *deadline))
                    .collect::<Vec<_>>();
                let batch = metadata_batch(store.decode_key_columns(&keys), &types, &values)?;
                if let Some(work) = &mut work {
                    work.try_resize(retained + batch.get_array_memory_size())?;
                }
                accept(batch)?;
                if let Some(work) = &mut work {
                    work.resize(0);
                }
                Ok(())
            });
        }

        let keys = self.metadata.keys().chain(
            self.cleanup_state
                .keys()
                .filter(|key| !self.metadata.contains_key(*key)),
        );
        let mut chunk = Vec::with_capacity(CHUNK_ROWS);
        let mut flush = |chunk: &mut Vec<&GroupKey>| -> Result<(), DataFusionError> {
            if chunk.is_empty() {
                return Ok(());
            }
            let values = chunk
                .iter()
                .map(|key| {
                    (
                        self.metadata.get(*key).unwrap_or(&empty),
                        self.cleanup_state.get(*key).copied(),
                    )
                })
                .collect::<Vec<_>>();
            let retained = values
                .iter()
                .map(|(meta, _)| meta.callbacks.len() * 8 + 32)
                .sum::<usize>()
                + chunk.iter().map(|key| group_key_bytes(key)).sum::<usize>();
            if let Some(work) = &mut work {
                work.try_resize(retained)?;
            }
            let keys = chunk.iter().map(|key| (*key).clone()).collect::<Vec<_>>();
            let batch = metadata_batch(key_columns(&keys, &types), &types, &values)?;
            if let Some(work) = &mut work {
                work.try_resize(retained + batch.get_array_memory_size())?;
            }
            accept(batch)?;
            chunk.clear();
            if let Some(work) = &mut work {
                work.resize(0);
            }
            Ok(())
        };
        for key in keys {
            chunk.push(key);
            if chunk.len() == CHUNK_ROWS {
                flush(&mut chunk)?;
            }
        }
        flush(&mut chunk)
    }

    pub(in crate::temporal_join) fn import_canonical_metadata(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        if !self.canonical_import_open {
            return Err(invalid(
                "metadata import requires an operator that has not started execution",
            ));
        }
        let count = self.left_keys.len();
        if batch.num_rows() > CHUNK_ROWS
            || batch.num_columns() != count + 4
            || batch.schema().fields()[..count]
                .iter()
                .zip(&self.left_keys)
                .any(|(actual, column)| {
                    actual.data_type() != self.left_schema.field(*column).data_type()
                })
        {
            return Err(invalid("metadata key schema or chunk size changed"));
        }
        let callbacks = batch
            .column(count + 3)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("missing physical callback list"))?;
        let key_arrays = batch.columns()[..count].iter().collect::<Vec<_>>();
        let mut unique = HashSet::default();
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            if !unique.insert(key.clone()) {
                return Err(invalid("duplicate metadata key"));
            }
            if callbacks.is_null(row) {
                return Err(invalid("NULL physical callback list"));
            }
            let array = callbacks.value(row);
            let times = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| invalid("invalid callback timestamp type"))?;
            if times.null_count() != 0 || times.values().windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(invalid("NULL, duplicate or unordered callbacks"));
            }
            let value = StockTemporalMeta {
                next_index: optional(batch, count, row)?,
                registered: optional(batch, count + 1, row)?,
                callbacks: times.values().iter().copied().collect(),
            };
            if value
                .registered
                .is_some_and(|time| !value.callbacks.contains(&time))
            {
                return Err(invalid("registered event value has no physical callback"));
            }
            let cleanup = optional(batch, count + 2, row)?;
            if cleanup.is_some() && !self.cleaning_enabled() {
                return Err(invalid("cleanup clock with disabled retention"));
            }
            if value.next_index.is_none()
                && value.registered.is_none()
                && value.callbacks.is_empty()
                && cleanup.is_none()
            {
                return Err(invalid("empty metadata record"));
            }
            rows.push((key, value, cleanup));
        }
        let mut work = self.memory.temporary_reservation();
        if let Some(work) = &mut work {
            work.try_grow(batch.get_array_memory_size())?;
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            let keys = store.entry_keys(
                batch,
                &(0..count).collect::<Vec<_>>(),
                &self.key_timestamp_precisions,
            );
            if keys
                .iter()
                .any(|key| store.all_metadata().contains_key(key) || store.deadline(key).is_some())
            {
                return Err(invalid("metadata key already imported"));
            }
            let values = rows
                .iter()
                .map(|(_, meta, _)| meta.encode())
                .collect::<Vec<_>>();
            let mut fields = batch.schema().fields()[..count].to_vec();
            fields.push(Arc::new(Field::new("metadata", DataType::Binary, false)));
            let mut columns = batch.columns()[..count].to_vec();
            columns.push(Arc::new(BinaryArray::from_iter_values(
                values.iter().map(Vec::as_slice),
            )));
            store.import_metadata(
                &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
                &self.key_timestamp_precisions,
            )?;
            for (key, (_, _, cleanup)) in keys.iter().zip(rows) {
                if let Some(deadline) = cleanup {
                    store.set_deadline(key, deadline)?;
                }
            }
            let delta = store.take_metadata_delta();
            drop(work);
            self.memory.record(delta);
            return self.memory.account();
        }
        if rows.iter().any(|(key, _, _)| {
            self.metadata.contains_key(key) || self.cleanup_state.contains_key(key)
        }) {
            return Err(invalid("metadata key already imported"));
        }
        let before = self.memory.state_bytes;
        let metadata_before = self.metadata_bytes();
        let mut deadline_bytes = 0;
        for (key, value, cleanup) in &rows {
            self.metadata.insert(key.clone(), value.clone());
            if let Some(deadline) = cleanup {
                self.cleanup_state.insert(key.clone(), *deadline);
                deadline_bytes += group_key_bytes(key);
            }
        }
        let delta = self.metadata_bytes() - metadata_before + deadline_bytes;
        drop(work);
        if self.memory.tracking() {
            self.memory.record(delta as isize);
        }
        if let Err(error) = self.memory.account() {
            for (key, _, _) in rows {
                self.metadata.remove(&key);
                self.cleanup_state.remove(&key);
            }
            self.memory.set(before);
            self.memory.account()?;
            return Err(error);
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportTemporalJoinCanonicalMetadata<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const TemporalJoiner) };
        if let Err(error) = joiner.visit_canonical_metadata(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importTemporalJoinCanonicalMetadata<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        let batch = import_record_batch(array, schema);
        if let Err(error) = joiner.import_canonical_metadata(&batch) {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindTemporalJoinCanonicalKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    descriptor: JIntArray<'local>,
) {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        let descriptor = read_i32_array(&env, &descriptor);
        if descriptor.len() != joiner.left_keys.len()
            || descriptor
                .iter()
                .any(|precision| !(-1..=9).contains(precision))
        {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                "invalid temporal key precision descriptor",
            );
            return;
        }
        joiner.key_timestamp_precisions = descriptor;
    });
}
