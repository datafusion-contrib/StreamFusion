use super::*;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical interval join: {message}"))
}

impl IntervalJoiner {
    fn canonical_schema(&self, left: bool) -> &SchemaRef {
        if left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        }
    }

    pub(super) fn visit_canonical_rows(
        &self,
        left: bool,
        mut accept: impl FnMut(RecordBatch, Vec<bool>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
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
        let (batches, matched) = if left {
            (&self.left_buffered, &self.left_matched)
        } else {
            (&self.right_buffered, &self.right_matched)
        };
        for batch in batches {
            for offset in (0..batch.num_rows()).step_by(CHUNK_ROWS) {
                let batch = batch.slice(offset, CHUNK_ROWS.min(batch.num_rows() - offset));
                let flags = if self.join_type == JoinKind::Inner {
                    vec![false; batch.num_rows()]
                } else {
                    let ids = batch
                        .column(batch.num_columns() - 1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| invalid("missing native row identities"))?;
                    ids.values().iter().map(|id| matched.contains(id)).collect()
                };
                let rows = RecordBatch::try_new(
                    schema.clone(),
                    batch.columns()[..schema.fields().len()].to_vec(),
                )?;
                accept(rows, flags)?;
            }
        }
        Ok(())
    }

    pub(super) fn import_canonical_rows(
        &mut self,
        left: bool,
        batch: &RecordBatch,
        matched: &[bool],
    ) -> Result<(), DataFusionError> {
        if self.operator_time != 0 || self.left_expiration != 0 || self.right_expiration != 0 {
            return Err(invalid("import requires fresh operator frontiers"));
        }
        let schema = self.canonical_schema(left).clone();
        if batch.num_rows() > CHUNK_ROWS
            || matched.len() != batch.num_rows()
            || batch.num_columns() != schema.fields().len()
            || batch
                .schema()
                .fields()
                .iter()
                .zip(schema.fields())
                .any(|(a, b)| a.data_type() != b.data_type())
        {
            return Err(invalid("retained row schema or transfer chunk changed"));
        }
        let time = if left {
            self.left_time
        } else {
            self.right_time
        };
        let times = rt_to_millis(batch.column(time));
        if times.null_count() != 0 {
            return Err(invalid("NULL interval cache timestamp"));
        }
        let mut work = self.memory.temporary_reservation();
        if let Some(work) = &mut work {
            work.try_grow(batch.get_array_memory_size())?;
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            let keys = if left {
                &self.left_keys
            } else {
                &self.right_keys
            }
            .clone();
            let precisions = self.key_timestamp_precisions.clone();
            self.store_mut()
                .push(left, batch, &keys, &precisions, &times, matched)?;
            self.store_timers_initialized = true;
            return Ok(());
        }
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                let data = column.to_data();
                let mut owned = MutableArrayData::new(vec![&data], false, column.len());
                owned.extend(0, 0, column.len());
                crate::owned::own_array_data(owned.freeze(), false).map(make_array)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut owned = RecordBatch::try_new(schema, columns)?;
        let start = if left {
            self.left_next_id
        } else {
            self.right_next_id
        };
        let next = start
            .checked_add(batch.num_rows() as i64)
            .ok_or_else(|| invalid("row identity overflow"))?;
        if self.join_type != JoinKind::Inner {
            let ids = Arc::new(Int64Array::from_iter_values(start..next));
            let mut columns = owned.columns().to_vec();
            columns.push(ids);
            owned = RecordBatch::try_new(with_rowid_schema(&owned.schema()), columns)?;
            let flags = if left {
                &mut self.left_matched
            } else {
                &mut self.right_matched
            };
            flags.extend(
                (start..next)
                    .zip(matched)
                    .filter_map(|(id, matched)| matched.then_some(id)),
            );
        }
        drop(work);
        if left {
            self.left_next_id = next;
        } else {
            self.right_next_id = next;
        }
        self.retain_arrival(owned, left);
        if let Err(error) = self.account() {
            let (batches, flags, next_id) = if left {
                (
                    &mut self.left_buffered,
                    &mut self.left_matched,
                    &mut self.left_next_id,
                )
            } else {
                (
                    &mut self.right_buffered,
                    &mut self.right_matched,
                    &mut self.right_next_id,
                )
            };
            batches.pop();
            flags.retain(|id| *id < start);
            *next_id = start;
            self.rebuild_index(left);
            self.account()?;
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn visit_canonical_timers(
        &mut self,
        left: bool,
        mut accept: impl FnMut(&[u8], i64) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.ensure_store_timers()?;
            return self
                .store
                .as_ref()
                .unwrap()
                .visit_canonical_timers(left, accept);
        }
        let timers = if left {
            &self.left_cleanup
        } else {
            &self.right_cleanup
        };
        for (key, deadline) in timers {
            accept(&key.0, *deadline)?;
        }
        Ok(())
    }

    pub(super) fn import_canonical_timer(
        &mut self,
        left: bool,
        key: &[u8],
        deadline: i64,
    ) -> Result<(), DataFusionError> {
        if self.operator_time != 0 || self.left_expiration != 0 || self.right_expiration != 0 {
            return Err(invalid("timer import requires fresh operator frontiers"));
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            let group = self
                .store
                .as_ref()
                .unwrap()
                .key_group(hash_bytes_by_words(key));
            self.store_mut()
                .set_cleanup_timer(left, group, key, Some(deadline))?;
            self.store_timers_initialized = true;
            return Ok(());
        }
        let timers = if left {
            &mut self.left_cleanup
        } else {
            &mut self.right_cleanup
        };
        let old = timers.insert(ByteKey::from(key), deadline);
        if let Err(error) = self.account() {
            let timers = if left {
                &mut self.left_cleanup
            } else {
                &mut self.right_cleanup
            };
            if let Some(old) = old {
                timers.insert(ByteKey::from(key), old);
            } else {
                timers.remove(key);
                timers.shrink_to_fit();
            }
            self.account()?;
            return Err(error);
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportIntervalJoinCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const IntervalJoiner) };
        let result = joiner.visit_canonical_rows(left != 0, |batch, flags| {
            let mut fields = batch.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(
                "__canonical_matched__",
                DataType::Boolean,
                false,
            )));
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(BooleanArray::from(flags)));
            crate::bridge::emit_record_batch(
                &mut env,
                &receiver,
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
            )
        });
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importIntervalJoinCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        let result = (|| {
            let batch = import_record_batch(array, schema);
            let count = joiner.canonical_schema(left != 0).fields().len();
            if batch.num_columns() != count + 1 {
                return Err(invalid("missing matched-row transfer flags"));
            }
            let flags = batch
                .column(count)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| invalid("invalid matched-row transfer flags"))?;
            if flags.null_count() != 0 {
                return Err(invalid("NULL matched-row transfer flags"));
            }
            let flags = flags.values().iter().collect::<Vec<_>>();
            joiner.import_canonical_rows(
                left != 0,
                &batch.project(&(0..count).collect::<Vec<_>>())?,
                &flags,
            )
        })();
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}

fn timer_batch(keys: &[Vec<u8>], deadlines: &[i64]) -> Result<RecordBatch, DataFusionError> {
    Ok(RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(BinaryArray::from_iter_values(
                keys.iter().map(Vec::as_slice),
            )) as ArrayRef,
        ),
        (
            "deadline",
            Arc::new(Int64Array::from(deadlines.to_vec())) as ArrayRef,
        ),
    ])?)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindIntervalJoinCanonicalKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    descriptor: JIntArray<'local>,
) {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        let descriptor = read_i32_array(&env, &descriptor);
        if descriptor.first() != Some(&i32::MIN) || descriptor.len() != joiner.left_keys.len() + 1 {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                "Invalid stock interval key descriptor",
            );
            return;
        }
        joiner.set_key_timestamp_precisions(descriptor);
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportIntervalJoinCanonicalTimers<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        let mut keys = Vec::with_capacity(CHUNK_ROWS);
        let mut deadlines = Vec::with_capacity(CHUNK_ROWS);
        let mut work = joiner.memory.temporary_reservation();
        let mut bytes = 0;
        let result = (|| {
            joiner.visit_canonical_timers(left != 0, |key, deadline| {
                bytes += key.len() + std::mem::size_of::<i64>();
                if let Some(work) = &mut work {
                    work.try_resize(bytes * 2)?;
                }
                keys.push(key.to_vec());
                deadlines.push(deadline);
                if keys.len() == CHUNK_ROWS {
                    crate::bridge::emit_record_batch(
                        &mut env,
                        &receiver,
                        timer_batch(&keys, &deadlines)?,
                    )?;
                    keys.clear();
                    deadlines.clear();
                    bytes = 0;
                    if let Some(work) = &mut work {
                        work.resize(0);
                    }
                }
                Ok(())
            })?;
            if !keys.is_empty() {
                crate::bridge::emit_record_batch(
                    &mut env,
                    &receiver,
                    timer_batch(&keys, &deadlines)?,
                )?;
            }
            Ok::<(), DataFusionError>(())
        })();
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importIntervalJoinCanonicalTimers<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        let result = (|| {
            let batch = import_record_batch(array, schema);
            if batch.num_columns() != 2 || batch.num_rows() > CHUNK_ROWS {
                return Err(invalid("invalid timer transfer chunk"));
            }
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("invalid timer key transfer"))?;
            let deadlines = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| invalid("invalid timer deadline transfer"))?;
            if keys.null_count() != 0 || deadlines.null_count() != 0 {
                return Err(invalid("NULL timer key or deadline"));
            }
            for row in 0..batch.num_rows() {
                joiner.import_canonical_timer(left != 0, keys.value(row), deadlines.value(row))?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}

#[cfg(test)]
pub(super) mod tests;
