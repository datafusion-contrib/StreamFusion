use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical group aggregate: {message}"))
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    fn configure_arithmetic(
        &mut self,
        kinds: &[i64],
        append_only: bool,
    ) -> Result<(), DataFusionError> {
        self.arithmetic_contract()?;
        if self.kinds != kinds {
            return Err(invalid("aggregate kinds differ from stock contract"));
        }
        if append_only && self.mini_batch {
            return Err(invalid("append-only state requires per-record aggregation"));
        }
        if self
            .stock_append_only
            .is_some_and(|mode| mode != append_only)
        {
            return Err(invalid(
                "group liveness mode differs from the configured stock contract",
            ));
        }
        self.stock_append_only = Some(append_only);
        Ok(())
    }

    fn arithmetic_contract(&self) -> Result<(), DataFusionError> {
        if self.kinds.is_empty()
            || self
                .kinds
                .iter()
                .any(|kind| !matches!(kind, 0 | 3 | 4 | 22))
            || self.state_types.iter().any(|ty| *ty != DataType::Int64)
            || self.record_count_column >= 0
            || self.count_columns.iter().any(|column| *column >= 0)
            || self.distinct_view_columns.iter().any(|column| *column >= 0)
        {
            return Err(invalid(
                "requires qualified single-phase BIGINT arithmetic state",
            ));
        }
        if self.staged_keys() != 0 || !self.deferred_batches.is_empty() {
            return Err(invalid(
                "pending aggregate input must be flushed before transfer",
            ));
        }
        Ok(())
    }

    fn transfer_columns<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<(&'a BinaryArray, Vec<&'a Int64Array>), DataFusionError> {
        self.arithmetic_contract()?;
        if batch.num_columns() != 3 + 2 * self.kinds.len() || batch.num_rows() > CHUNK_ROWS {
            return Err(invalid("invalid transfer shape"));
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid binary keys"))?;
        let values = batch.columns()[1..]
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("invalid accumulator field type"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fixed = (self.key_columns.len() + 8).div_ceil(64) * 8 + self.key_columns.len() * 8;
        let mut seen = std::collections::HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if keys.is_null(row)
                || keys.value(row).len() < fixed
                || keys.value(row).len() % 8 != 0
                || keys.value(row)[0] != 0
                || !seen.insert(keys.value(row))
                || values[0].is_null(row)
                || (self.stock_append_only == Some(true) && values[0].value(row) != 1)
                || values[1].is_null(row)
                || (self.ttl_ms == 0 && values[1].value(row) != 0)
            {
                return Err(invalid("malformed or duplicate group metadata"));
            }
            for (i, kind) in self.kinds.iter().enumerate() {
                if values[3 + 2 * i].is_null(row) || (*kind != 0 && values[2 + 2 * i].is_null(row))
                {
                    return Err(invalid("NULL in a required accumulator field"));
                }
                if self.stock_append_only == Some(true)
                    && *kind == 0
                    && values[3 + 2 * i].value(row) != i64::from(!values[2 + 2 * i].is_null(row))
                {
                    return Err(invalid(
                        "append-only SUM must retain initialization presence",
                    ));
                }
            }
        }
        Ok((keys, values))
    }

    fn transfer_arithmetic(
        &mut self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let (keys, values) = self.transfer_columns(batch)?;
        let result = (|| {
            self.store.prepare_canonical_import(keys)?;
            if (0..batch.num_rows()).any(|row| self.store.contains(keys.value(row))) {
                return Err(invalid(
                    "duplicate group in existing target or earlier chunk",
                ));
            }
            if validating {
                return Ok(());
            }
            self.snapshot_cache = None;
            for row in 0..batch.num_rows() {
                let mut aggs = Vec::with_capacity(self.kinds.len());
                for (i, (kind, ty)) in self.kinds.iter().zip(&self.accumulator_types).enumerate() {
                    let mut state = GroupAggState::new(*kind, ty);
                    let GroupAggState::Running { agg, non_null } = &mut state else {
                        return Err(invalid("unexpected non-arithmetic state"));
                    };
                    let value = values[2 + 2 * i];
                    agg.restore_value(&ScalarValue::Int64(
                        (!value.is_null(row)).then(|| value.value(row)),
                    ));
                    *non_null = values[3 + 2 * i].value(row);
                    aggs.push(state);
                }
                let state = GroupKeyState {
                    aggs,
                    records: values[0].value(row),
                    last_output: None,
                    last_output_bytes: 0,
                    last_write_ms: values[1].value(row),
                    distinct_retention: None,
                };
                if self.memory.tracking() {
                    self.memory.record(
                        (byte_key_bytes(keys.value(row)) + group_key_state_bytes(&state)) as isize,
                    );
                }
                self.store.insert(ByteKey::from(keys.value(row)), state);
            }
            self.memory.account()
        })();
        let cleanup = self.store.end_bundle();
        self.memory.record(self.store.footprint_delta());
        let accounted = self.memory.account();
        result.and(cleanup).and(accounted)
    }
}

fn transfer_batch(
    kinds: &[i64],
    ttl: bool,
    append_only: bool,
    entries: &[(&ByteKey, &GroupKeyState)],
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = vec![
        Field::new("binary_key", DataType::Binary, false),
        Field::new("records", DataType::Int64, false),
        Field::new("written", DataType::Int64, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from_iter_values(
            entries.iter().map(|(key, _)| key.0.as_ref()),
        )),
        Arc::new(Int64Array::from_iter_values(
            entries
                .iter()
                .map(|(_, state)| if append_only { 1 } else { state.records }),
        )),
        Arc::new(Int64Array::from_iter_values(
            entries
                .iter()
                .map(|(_, state)| if ttl { state.last_write_ms } else { 0 }),
        )),
    ];
    for (i, kind) in kinds.iter().enumerate() {
        let mut values = Vec::with_capacity(entries.len());
        let mut counts = Vec::with_capacity(entries.len());
        for (_, state) in entries {
            let GroupAggState::Running { agg, non_null } = &state.aggs[i] else {
                return Err(invalid("unexpected non-arithmetic retained state"));
            };
            let ScalarValue::Int64(value) = agg.emit() else {
                return Err(invalid("unexpected raw accumulator type"));
            };
            if *kind != 0 && value.is_none() {
                return Err(invalid("NULL in a required retained accumulator"));
            }
            values.push(value);
            counts.push(if append_only && *kind == 0 {
                i64::from(value.is_some())
            } else {
                *non_null
            });
        }
        fields.push(Field::new(format!("value{i}"), DataType::Int64, true));
        fields.push(Field::new(format!("count{i}"), DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(values)));
        columns.push(Arc::new(Int64Array::from(counts)));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn visit<'a>(
    kinds: &[i64],
    ttl: bool,
    append_only: bool,
    entries: impl Iterator<Item = (&'a ByteKey, &'a GroupKeyState)>,
    mut receiver: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    let mut chunk = Vec::with_capacity(CHUNK_ROWS);
    for entry in entries {
        chunk.push(entry);
        if chunk.len() == CHUNK_ROWS {
            receiver(transfer_batch(kinds, ttl, append_only, &chunk)?)?;
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        receiver(transfer_batch(kinds, ttl, append_only, &chunk)?)?;
    }
    Ok(())
}

fn emit(
    env: &mut JNIEnv,
    receiver: &JObject,
    batch: RecordBatch,
    memory: &mut Option<MemoryReservation>,
) -> Result<(), DataFusionError> {
    if let Some(memory) = memory {
        memory.try_resize(batch.get_array_memory_size())?;
    }
    let result = crate::bridge::emit_record_batch(env, receiver, batch);
    let released = memory
        .as_mut()
        .map_or(Ok(()), |memory| memory.try_resize(0));
    result.and(released)
}

fn throw_error(env: &mut JNIEnv, error: DataFusionError) {
    if env.exception_check().unwrap_or(true) {
        return;
    }
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureGroupArithmeticCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    kinds: JIntArray<'local>,
) {
    configure(env, handle, persistent, kinds, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureGroupArithmeticCanonicalMode<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    kinds: JIntArray<'local>,
    append_only: jboolean,
) {
    configure(env, handle, persistent, kinds, append_only != 0);
}

fn configure<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    persistent: jboolean,
    kinds: JIntArray<'local>,
    append_only: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let expected = read_int_array(&env, &kinds);
        let result = if persistent == 0 {
            unsafe { &mut *(handle as *mut GroupAggregator) }
                .configure_arithmetic(&expected, append_only)
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) }
                    .configure_arithmetic(&expected, append_only)
            }
            #[cfg(not(feature = "rocksdb-state"))]
            Err(invalid("native RocksDB is unavailable"))
        };
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportGroupArithmeticCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = (|| {
            if persistent == 0 {
                let owner = unsafe { &*(handle as *const GroupAggregator) };
                owner.arithmetic_contract()?;
                let mut memory = owner.memory.temporary_reservation();
                visit(
                    &owner.kinds,
                    owner.ttl_ms > 0,
                    owner.stock_append_only == Some(true),
                    owner.store.iter(),
                    |batch| emit(&mut env, &receiver, batch, &mut memory),
                )
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let owner = unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) };
                    owner.arithmetic_contract()?;
                    let mut decoded = owner.memory.temporary_reservation();
                    let mut memory = owner.memory.temporary_reservation();
                    let kinds = &owner.kinds;
                    let ttl = owner.ttl_ms > 0;
                    let append_only = owner.stock_append_only == Some(true);
                    owner.store.for_each_canonical_batch(1, |entries| {
                        if let Some(memory) = &mut decoded {
                            memory.try_resize(
                                entries
                                    .iter()
                                    .map(|(key, value)| {
                                        byte_key_bytes(&key.0) + group_key_state_bytes(value)
                                    })
                                    .sum(),
                            )?;
                        }
                        let result = visit(
                            kinds,
                            ttl,
                            append_only,
                            entries.iter().map(|(k, v)| (k, v)),
                            |batch| emit(&mut env, &receiver, batch, &mut memory),
                        );
                        let released = decoded
                            .as_mut()
                            .map_or(Ok(()), |memory| memory.try_resize(0));
                        result.and(released)
                    })
                }
                #[cfg(not(feature = "rocksdb-state"))]
                Err(invalid("native RocksDB is unavailable"))
            }
        })();
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importGroupArithmeticCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, array, schema, false);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateGroupArithmeticCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, array, schema, true);
}

fn transfer<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    persistent: jboolean,
    array: jlong,
    schema: jlong,
    validating: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let batch = import_record_batch(array, schema);
        let result = if persistent == 0 {
            unsafe { &mut *(handle as *mut GroupAggregator) }
                .transfer_arithmetic(&batch, validating)
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) }
                    .transfer_arithmetic(&batch, validating)
            }
            #[cfg(not(feature = "rocksdb-state"))]
            Err(invalid("native RocksDB is unavailable"))
        };
        // Arrow's release callback can enter the JVM. Release it before raising an exception.
        drop(batch);
        if let Err(error) = result {
            throw_error(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;
