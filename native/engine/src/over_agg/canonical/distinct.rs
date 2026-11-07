use super::*;
use jni::sys::jint;

impl OverWindowAggregator {
    pub(in crate::over_agg) fn ensure_canonical_distinct_ready(
        &self,
    ) -> Result<(), DataFusionError> {
        if self.canonical_distinct_loading {
            return Err(invalid("DISTINCT membership restore is not finalized"));
        }
        Ok(())
    }

    fn bind_canonical_distinct(&mut self, index: usize) -> Result<(), DataFusionError> {
        let OverInner::Aggregates(inner) = &self.inner else {
            return Err(invalid("DISTINCT requires an unbounded fold"));
        };
        let kind = *inner
            .kinds
            .get(index)
            .ok_or_else(|| invalid("DISTINCT index is out of range"))?;
        if kind < 100 || !matches!(kind - 100, 0 | 1 | 2 | 3 | 4 | 22) {
            return Err(invalid("aggregate has no qualified DISTINCT membership"));
        }
        self.canonical_distinct.insert(index);
        Ok(())
    }

    fn canonical_distinct_type(&self, index: usize) -> Result<&DataType, DataFusionError> {
        let inner = self.canonical_fold()?;
        if !self.canonical_distinct.contains(&index) {
            return Err(invalid("DISTINCT membership index is not bound"));
        }
        inner
            .value_types
            .get(index)
            .ok_or_else(|| invalid("DISTINCT index is out of range"))
    }

    fn distinct_batch(
        &self,
        index: usize,
        keys: &[Box<[u8]>],
        values: Vec<ScalarValue>,
    ) -> Result<RecordBatch, DataFusionError> {
        let inner = self.canonical_fold()?;
        let mut fields = key_fields(&inner.key_types);
        fields.push(Field::new(
            "value",
            self.canonical_distinct_type(index)?.clone(),
            false,
        ));
        let refs = keys.iter().map(|key| key.as_ref()).collect::<Vec<_>>();
        let mut columns = decode_byte_keys(inner.key_converter.as_ref(), &refs, &inner.key_types);
        columns.push(scalars_to_array(
            values,
            self.canonical_distinct_type(index)?,
        ));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn visit_canonical_distinct(
        &self,
        index: usize,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.ensure_canonical_distinct_ready()?;
        self.canonical_distinct_type(index)?;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let slot = self
                .distinct_slots
                .iter()
                .position(|&aggregate| aggregate == index)
                .ok_or_else(|| invalid("DISTINCT store slot is missing"))?;
            return store.visit_all_distinct(slot, &self.store_key_types, CHUNK_ROWS, |members| {
                let (keys, values): (Vec<_>, Vec<_>) = members.into_iter().unzip();
                accept(self.distinct_batch(index, &keys, values)?)
            });
        }
        let mut keys = Vec::with_capacity(CHUNK_ROWS);
        let mut values = Vec::with_capacity(CHUNK_ROWS);
        let inner = self.canonical_fold()?;
        for (key, states) in &inner.keys {
            let seen = states[index]
                .distinct
                .as_ref()
                .ok_or_else(|| invalid("DISTINCT set is missing"))?;
            seen.for_each_scalar(|value, count| {
                if count <= 0 {
                    return Err(invalid("nonpositive unbounded DISTINCT membership"));
                }
                keys.push(key.0.clone());
                values.push(value);
                if values.len() == CHUNK_ROWS {
                    accept(self.distinct_batch(index, &keys, std::mem::take(&mut values))?)?;
                    keys.clear();
                }
                Ok(())
            })?;
        }
        if !values.is_empty() {
            accept(self.distinct_batch(index, &keys, values)?)?;
        }
        Ok(())
    }

    fn import_canonical_distinct(
        &mut self,
        index: usize,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let value_type = self.canonical_distinct_type(index)?.clone();
        let arity = self.key_columns.len();
        if batch.num_columns() != arity + 1
            || batch.num_rows() > CHUNK_ROWS
            || batch.column(arity).data_type() != &value_type
            || batch.column(arity).null_count() != 0
        {
            return Err(invalid(
                "invalid DISTINCT membership schema or NULL element",
            ));
        }
        let independent = self.independent_distinct_ttl();
        if !self.canonical_distinct_loading && !independent {
            return Err(invalid("DISTINCT membership has no pending fold restore"));
        }
        let arrays = (0..arity)
            .map(|column| batch.column(column))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        let inner = self.canonical_fold()?;
        if types != inner.key_types
            && !(independent && inner.keys.is_empty() && inner.key_types.is_empty())
        {
            return Err(invalid("DISTINCT partition key type changed"));
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let mut local: HashMap<ByteKey, DistinctSet> = HashMap::default();
        let mut values = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(keys.row(row).data());
            let value = ScalarValue::try_from_array(batch.column(arity), row)?;
            if !local
                .entry(key)
                .or_insert_with(|| DistinctSet::new(&value_type))
                .add_scalar(value.clone())
            {
                return Err(invalid("duplicate DISTINCT membership in batch"));
            }
            values.push(value);
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            let slot = self
                .distinct_slots
                .iter()
                .position(|&aggregate| aggregate == index)
                .ok_or_else(|| invalid("DISTINCT store slot is missing"))?;
            let elements = store.encode_distinct_elements(slot, batch.column(arity))?;
            let key_columns = (0..arity).collect::<Vec<_>>();
            let mut entries = Vec::with_capacity(batch.num_rows());
            let mut folds = Vec::with_capacity(batch.num_rows());
            for (row, element) in elements.into_iter().enumerate() {
                let key = keys.row(row).data();
                let group = store.key_group(binary_row_hash(
                    batch,
                    &key_columns,
                    row,
                    &self.key_timestamp_precisions,
                ));
                let mut entry = store.distinct_prefix(slot, group, key);
                entry.extend_from_slice(&element);
                entries.push(entry);
                folds.push(store.fold_key(group, key));
            }
            if !independent && store.get_folds(&folds)?.iter().any(Option::is_none) {
                return Err(invalid("DISTINCT membership has no accumulator"));
            }
            if store.probe_distinct(&entries)?.iter().any(|seen| *seen) {
                return Err(invalid("duplicate retained DISTINCT membership"));
            }
            self.store.as_mut().unwrap().write_distinct(&entries)?;
            self.canonical_distinct_loading = true;
            return self.account();
        }
        for row in 0..batch.num_rows() {
            let state = inner
                .keys
                .get(keys.row(row).data())
                .and_then(|states| states.get(index));
            if state.is_none() && !independent {
                return Err(invalid("DISTINCT membership has no accumulator"));
            }
            if state.is_some_and(|state| {
                state.distinct.as_ref().unwrap().scalar_count(&values[row]) != 0
            }) {
                return Err(invalid("duplicate retained DISTINCT membership"));
            }
        }
        self.inner.seed_key_codec(&types);
        let OverInner::Aggregates(inner) = &mut self.inner else {
            unreachable!()
        };
        for (row, value) in values.into_iter().enumerate() {
            inner.states(keys.row(row).data())[index].remember_distinct(value);
        }
        inner.recompute_bytes();
        self.canonical_distinct_loading = true;
        self.account()
    }

    fn validate_distinct_state(
        &self,
        index: usize,
        count: usize,
        state: &[ScalarValue],
    ) -> Result<(), DataFusionError> {
        let inner = self.canonical_fold()?;
        let offset = inner
            .kinds
            .iter()
            .zip(&inner.value_types)
            .take(index)
            .map(|(&kind, ty)| OverAggState::new(kind, ty).state_types().len())
            .sum::<usize>();
        let kind = inner.kinds[index] - 100;
        if self.independent_distinct_ttl() {
            if matches!(kind, 3 | 4) {
                let ScalarValue::Int64(Some(current)) = state[offset + usize::from(kind == 4)]
                else {
                    return Err(invalid("invalid DISTINCT count"));
                };
                if current < 0 || current as u64 > count as u64 {
                    return Err(invalid("DISTINCT count exceeds lifetime membership"));
                }
            } else if count == 0 && kind != 22 && !state[offset].is_null() {
                return Err(invalid("DISTINCT scalar has no lifetime membership"));
            } else if count == 0
                && kind == 22
                && state[offset] != ScalarValue::new_zero(&state[offset].data_type())?
            {
                return Err(invalid(
                    "DISTINCT zero-initialized sum disagrees with membership",
                ));
            }
            return Ok(());
        }
        if kind == 3 {
            if state[offset] != ScalarValue::Int64(Some(count as i64)) {
                return Err(invalid("DISTINCT count disagrees with membership"));
            }
        } else if kind == 4 {
            if state[offset + 1] != ScalarValue::Int64(Some(count as i64)) {
                return Err(invalid("DISTINCT AVG count disagrees with membership"));
            }
        } else if kind == 0 {
            if (count == 0 && !state[offset].is_null())
                || (count > 0
                    && state[offset].is_null()
                    && !matches!(inner.value_types[index], DataType::Decimal128(..)))
            {
                return Err(invalid("DISTINCT sum emptiness disagrees with membership"));
            }
        } else if kind == 22 {
            if count == 0 && state[offset] != ScalarValue::new_zero(&state[offset].data_type())? {
                return Err(invalid(
                    "DISTINCT zero-initialized sum disagrees with membership",
                ));
            }
        } else if matches!(kind, 1 | 2) && (count == 0) != state[offset].is_null() {
            return Err(invalid(
                "DISTINCT extrema emptiness disagrees with membership",
            ));
        }
        Ok(())
    }

    fn finish_canonical_distinct(&mut self) -> Result<(), DataFusionError> {
        let inner = self.canonical_fold()?;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            store.visit_folds(CHUNK_ROWS, |folds| {
                for fold in folds {
                    for &index in &self.canonical_distinct {
                        let slot = self
                            .distinct_slots
                            .iter()
                            .position(|&aggregate| aggregate == index)
                            .unwrap();
                        let mut count = 0;
                        store.visit_distinct(
                            slot,
                            self.store_fold_key_group(&fold.key),
                            &fold.key,
                            CHUNK_ROWS,
                            |values| {
                                count += values.len();
                                Ok(())
                            },
                        )?;
                        self.validate_distinct_state(index, count, &fold.state)?;
                    }
                }
                Ok(())
            })?;
            self.canonical_distinct_loading = false;
            return self.account();
        }
        for (key, states) in &inner.keys {
            if self.independent_distinct_ttl() && !self.last_write_ms.contains_key(key) {
                continue;
            }
            let scalars = states
                .iter()
                .flat_map(OverAggState::state)
                .collect::<Vec<_>>();
            for &index in &self.canonical_distinct {
                self.validate_distinct_state(
                    index,
                    states[index].distinct.as_ref().unwrap().len(),
                    &scalars,
                )?;
            }
        }
        self.canonical_distinct_loading = false;
        self.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindOverCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = usize::try_from(index)
            .map_err(|_| invalid("negative DISTINCT index"))
            .and_then(|index| {
                unsafe { &mut *(handle as *mut OverWindowAggregator) }
                    .bind_canonical_distinct(index)
            });
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = usize::try_from(index)
            .map_err(|_| invalid("negative DISTINCT index"))
            .and_then(|index| {
                unsafe { &*(handle as *const OverWindowAggregator) }
                    .visit_canonical_distinct(index, |batch| {
                        crate::bridge::emit_record_batch(&mut env, &receiver, batch)
                    })
            });
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index: jint,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = {
            let batch = import_record_batch(array, schema);
            usize::try_from(index)
                .map_err(|_| invalid("negative DISTINCT index"))
                .and_then(|index| {
                    unsafe { &mut *(handle as *mut OverWindowAggregator) }
                        .import_canonical_distinct(index, &batch)
                })
        };
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_finishOverCanonicalDistinct<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        if let Err(error) =
            unsafe { &mut *(handle as *mut OverWindowAggregator) }.finish_canonical_distinct()
        {
            report(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "rocksdb-state"))]
mod ttl_tests;
