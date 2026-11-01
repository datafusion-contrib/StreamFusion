use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;
const STAMP: &str = "__over_retention_stamp";

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical OVER fold: {message}"))
}

impl OverWindowAggregator {
    fn canonical_fold(&self) -> Result<&OverAggregator, DataFusionError> {
        let OverInner::Aggregates(inner) = &self.inner else {
            return Err(invalid(
                "operator has frame or window-function state, not an unbounded fold",
            ));
        };
        if inner.kinds.iter().any(|kind| *kind >= 100) {
            return Err(invalid(
                "DISTINCT state also requires a membership-view transfer",
            ));
        }
        Ok(inner)
    }

    fn fold_batch(
        &self,
        keys: &[&[u8]],
        states: &[Vec<ScalarValue>],
        stamps: Vec<i64>,
    ) -> Result<RecordBatch, DataFusionError> {
        let inner = self.canonical_fold()?;
        let types = inner.state_types();
        let key_types = &inner.key_types;
        let mut fields = key_fields(key_types);
        let mut columns = decode_byte_keys(inner.key_converter.as_ref(), keys, key_types);
        for (index, data_type) in types.iter().enumerate() {
            fields.push(Field::new(format!("state{index}"), data_type.clone(), true));
            columns.push(scalars_to_array(
                states.iter().map(|row| row[index].clone()).collect(),
                data_type,
            ));
        }
        fields.push(Field::new(STAMP, DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(stamps)));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn visit_canonical_folds(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let inner = self.canonical_fold()?;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            if !inner.keys.is_empty() {
                return Err(invalid("persistent operator has undrained resident folds"));
            }
            return store.visit_folds(CHUNK_ROWS, |chunk| {
                let keys = chunk.iter().map(|row| row.key.as_ref()).collect::<Vec<_>>();
                let states = chunk
                    .iter()
                    .map(|row| row.state.clone())
                    .collect::<Vec<_>>();
                let stamps = chunk.iter().map(|row| row.stamp).collect();
                accept(self.fold_batch(&keys, &states, stamps)?)
            });
        }
        let mut keys = Vec::with_capacity(CHUNK_ROWS);
        let mut states = Vec::with_capacity(CHUNK_ROWS);
        let mut stamps = Vec::with_capacity(CHUNK_ROWS);
        for (key, values) in &inner.keys {
            keys.push(key.0.as_ref());
            states.push(values.iter().flat_map(OverAggState::state).collect());
            stamps.push(self.fold_stamp(&key.0));
            if keys.len() == CHUNK_ROWS {
                accept(self.fold_batch(&keys, &states, std::mem::take(&mut stamps))?)?;
                keys.clear();
                states.clear();
            }
        }
        if !keys.is_empty() {
            accept(self.fold_batch(&keys, &states, stamps)?)?;
        }
        Ok(())
    }

    fn import_canonical_folds(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let inner = self.canonical_fold()?;
        let types = inner.state_types();
        let arity = self.key_columns.len();
        if batch.num_columns() != arity + types.len() + 1 {
            return Err(invalid("unexpected fold schema arity"));
        }
        for (index, data_type) in types.iter().enumerate() {
            if batch.column(arity + index).data_type() != data_type {
                return Err(invalid("raw accumulator type changed"));
            }
        }
        if batch.schema().field(arity + types.len()).name() != STAMP {
            return Err(invalid("retention stamp is missing"));
        }
        let Some(stamps) = batch
            .column(arity + types.len())
            .as_any()
            .downcast_ref::<Int64Array>()
        else {
            return Err(invalid("retention stamp must be BIGINT"));
        };
        if stamps.null_count() != 0 {
            return Err(invalid("NULL retention stamp"));
        }
        let mut offset = arity;
        for (&kind, value_type) in inner.kinds.iter().zip(&inner.value_types) {
            let state = OverAggState::new(kind, value_type);
            if matches!(
                &state.agg,
                RunningAgg::Count(_)
                    | RunningAgg::AvgInt { .. }
                    | RunningAgg::AvgFloat { .. }
                    | RunningAgg::ZeroSumF32(_)
                    | RunningAgg::ZeroSumF64(_)
                    | RunningAgg::ZeroSumInt { .. }
            ) && batch.column(offset).null_count() != 0
            {
                return Err(invalid("NULL in a required raw accumulator"));
            }
            if state.has_count_state() && batch.column(offset + 1).null_count() != 0 {
                return Err(invalid("NULL aggregate count"));
            }
            offset += state.state_types().len();
        }
        let arrays = (0..arity)
            .map(|index| batch.column(index))
            .collect::<Vec<_>>();
        let key_types = key_types(&arrays);
        if !inner.key_types.is_empty() && inner.key_types != key_types {
            return Err(invalid("partition key type changed"));
        }
        let mut converter = Some(RowConverter::new(if key_types.is_empty() {
            vec![SortField::new(DataType::Boolean)]
        } else {
            key_types.iter().cloned().map(SortField::new).collect()
        })?);
        let encoded = encode_keys(&mut converter, &arrays, batch.num_rows());
        let mut seen = ahash::HashSet::default();
        let mut states = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = encoded.row(row).data();
            if !seen.insert(key.to_vec()) || inner.keys.contains_key(key) {
                return Err(invalid("duplicate retained partition"));
            }
            states.push(
                (0..types.len())
                    .map(|index| ScalarValue::try_from_array(batch.column(arity + index), row))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            let key_columns = (0..arity).collect::<Vec<_>>();
            let entries = (0..batch.num_rows())
                .map(|row| {
                    let group = store.key_group(binary_row_hash(
                        batch,
                        &key_columns,
                        row,
                        &self.key_timestamp_precisions,
                    ));
                    (
                        store.fold_key(group, encoded.row(row).data()),
                        stamps.value(row),
                    )
                })
                .collect::<Vec<_>>();
            if store
                .get_folds(
                    &entries
                        .iter()
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>(),
                )?
                .iter()
                .any(Option::is_some)
            {
                return Err(invalid("duplicate retained partition"));
            }
            let columns = (arity..arity + types.len())
                .map(|index| batch.column(index).clone())
                .collect::<Vec<_>>();
            store.write_folds(&entries, &columns)?;
        } else {
            self.install_memory_folds(&encoded, &states, &key_types);
        }
        #[cfg(not(feature = "rocksdb-state"))]
        self.install_memory_folds(&encoded, &states, &key_types);
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(encoded.row(row).data());
            if self.value_ttl_on() {
                let bytes = byte_key_bytes(&key.0);
                if self.last_write_ms.insert(key, stamps.value(row)).is_none() {
                    self.retention_bytes += bytes;
                }
            } else if self.deadline_cleaning() {
                let bytes = byte_key_bytes(&key.0);
                if self.cleanup_state.insert(key, stamps.value(row)).is_none() {
                    self.retention_bytes += bytes;
                }
            }
        }
        self.account()
    }

    fn install_memory_folds(
        &mut self,
        keys: &arrow::row::Rows,
        states: &[Vec<ScalarValue>],
        key_types: &[DataType],
    ) {
        self.inner.seed_key_codec(key_types);
        let OverInner::Aggregates(inner) = &mut self.inner else {
            unreachable!()
        };
        for (row, scalars) in states.iter().enumerate() {
            let mut offset = 0;
            for state in inner.states(keys.row(row).data()) {
                let count = state.state_types().len();
                state.restore_state(&scalars[offset..offset + count]);
                offset += count;
            }
        }
        inner.recompute_bytes();
    }
}

fn report(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverFoldCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        if let Err(error) = operator.visit_canonical_folds(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverFoldCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let result = {
            let batch = import_record_batch(array, schema);
            operator.import_canonical_folds(&batch)
        };
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;
