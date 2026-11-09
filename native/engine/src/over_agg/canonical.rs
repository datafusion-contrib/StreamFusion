use super::*;
use jni::objects::JObject;

const CHUNK_ROWS: usize = 1024;
const STAMP: &str = "__over_retention_stamp";

mod distinct;
mod event;
mod frame;
mod proc_rows;
mod range;

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
        if inner
            .kinds
            .iter()
            .enumerate()
            .any(|(index, kind)| *kind >= 100 && !self.canonical_distinct.contains(&index))
        {
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
        fold_batch(
            &inner.key_types,
            &inner.state_types(),
            inner.key_converter.as_ref(),
            keys,
            states,
            stamps,
        )
    }

    fn visit_canonical_folds(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.ensure_canonical_distinct_ready()?;
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
            if self.independent_distinct_ttl() && !self.last_write_ms.contains_key(key) {
                continue;
            }
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
        self.transfer_canonical_folds(batch, false)
    }

    fn validate_canonical_folds(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.transfer_canonical_folds(batch, true)
    }

    fn transfer_canonical_folds(
        &mut self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
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
            if !validating {
                store.write_folds(&entries, &columns)?;
            }
        } else if !validating {
            self.install_memory_folds(&encoded, &states, &key_types);
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if !validating {
            self.install_memory_folds(&encoded, &states, &key_types);
        }
        if validating {
            return Ok(());
        }
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
        if !self.canonical_distinct.is_empty() {
            self.canonical_distinct_loading = true;
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

fn fold_batch(
    key_types: &[DataType],
    types: &[DataType],
    converter: Option<&RowConverter>,
    keys: &[&[u8]],
    states: &[Vec<ScalarValue>],
    stamps: Vec<i64>,
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = key_fields(key_types);
    let mut columns = decode_byte_keys(converter, keys, key_types);
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

fn report(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
    }
}

#[cfg(feature = "rocksdb-state")]
struct OverFoldCursor {
    snapshot: crate::state::rocks_store::over_agg_store::RocksOverFoldSnapshot,
    keys: Vec<DataType>,
    types: Vec<DataType>,
    converter: RowConverter,
}

#[cfg(feature = "rocksdb-state")]
impl crate::state::canonical_snapshot::CanonicalCursor for OverFoldCursor {
    fn next(&mut self, env: &mut JNIEnv, receiver: &JObject) -> Result<bool, DataFusionError> {
        let rows = self.snapshot.next_batch()?;
        if rows.is_empty() {
            return Ok(false);
        }
        let keys: Vec<_> = rows.iter().map(|row| row.key.as_ref()).collect();
        let states: Vec<_> = rows.iter().map(|row| row.state.clone()).collect();
        let stamps = rows.iter().map(|row| row.stamp).collect();
        crate::bridge::emit_record_batch(
            env,
            receiver,
            fold_batch(
                &self.keys,
                &self.types,
                Some(&self.converter),
                &keys,
                &states,
                stamps,
            )?,
        )?;
        Ok(true)
    }
}

#[cfg(feature = "rocksdb-state")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_captureOverFoldCanonicalSnapshot<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let owner = unsafe { &*(handle as *const OverWindowAggregator) };
        let result = (|| {
            owner.ensure_canonical_distinct_ready()?;
            let inner = owner.canonical_fold()?;
            if !inner.keys.is_empty() {
                return Err(invalid("persistent operator has undrained resident folds"));
            }
            let store = owner
                .store
                .as_ref()
                .ok_or_else(|| invalid("MVCC capture requires RocksDB state"))?;
            let keys = inner.key_types.clone();
            let types = inner.state_types();
            let converter = RowConverter::new(
                keys.iter()
                    .map(|data_type| SortField::new(data_type.clone()))
                    .collect(),
            )?;
            let cursor = OverFoldCursor {
                snapshot: store.canonical_fold_snapshot(&types)?,
                keys,
                types,
                converter,
            };
            Ok::<_, DataFusionError>(Box::into_raw(Box::new(
                crate::state::canonical_snapshot::CanonicalCursorHandle(Box::new(cursor)),
            )) as jlong)
        })();
        match result {
            Ok(handle) => handle,
            Err(error) => {
                report(&mut env, error);
                0
            }
        }
    })
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

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateOverFoldCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = {
            let batch = import_record_batch(array, schema);
            match unsafe { (handle as *mut OverWindowAggregator).as_mut() } {
                Some(operator) => operator.validate_canonical_folds(&batch),
                None => Err(invalid("missing target operator")),
            }
        };
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;

#[cfg(feature = "rocksdb-state")]
impl OverWindowAggregator {
    fn frozen_store(&self) -> Option<&crate::state::RocksOverAggStore> {
        self.store.as_ref().filter(|store| store.is_frozen())
    }
}

impl OverWindowAggregator {
    fn canonical_cleanup_stamp(&self, key: &[u8]) -> Result<Option<i64>, DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = self.frozen_store() {
            return Ok(store
                .cleanup_stamp(self.store_fold_key_group(key), key)?
                .map(|(stamp, _)| stamp));
        }
        Ok(self.cleanup_state.get(key).copied())
    }

    fn canonical_range_timers(&self, key: &[u8]) -> Result<Option<RangeTimers>, DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = self.frozen_store() {
            return store
                .range_timer_state(self.store_fold_key_group(key), key)?
                .map(|value| RangeTimers::decode(&value))
                .transpose();
        }
        Ok(self.range_timers.get(key).cloned())
    }
}

fn captured_key_converter(
    source: &Option<RowConverter>,
    types: &[DataType],
) -> Option<RowConverter> {
    source.as_ref().map(|_| key_row_converter_from_types(types))
}

impl crate::state::canonical_memory::CanonicalMemoryState for OverWindowAggregator {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError> {
        target.memory.set(self.memory.state_bytes);
        target.memory.account()?;
        match (&self.inner, &mut target.inner) {
            (OverInner::Aggregates(source), OverInner::Aggregates(target)) => {
                target.value_types = source.value_types.clone();
                target.keys = source.keys.clone();
                target.key_types = source.key_types.clone();
                target.key_converter =
                    captured_key_converter(&source.key_converter, &source.key_types);
                target.bytes = source.bytes;
            }
            (OverInner::Bounded(source), OverInner::Bounded(target)) => {
                target.value_types = source.value_types.clone();
                target.fold_types = source.fold_types.clone();
                target.range_order = source.range_order.clone();
                target.keys = source.keys.clone();
                target.key_types = source.key_types.clone();
                target.key_converter =
                    captured_key_converter(&source.key_converter, &source.key_types);
                target.bytes = source.bytes;
            }
            (OverInner::WindowFunctions(source), OverInner::WindowFunctions(target)) => {
                target.keys = source.keys.clone();
                target.key_types = source.key_types.clone();
                target.key_converter =
                    captured_key_converter(&source.key_converter, &source.key_types);
                target.bytes = source.bytes;
            }
            _ => return Err(invalid("incompatible memory capture handles")),
        }
        target.buffered = self.buffered.clone();
        target.input_schema = self.input_schema.clone();
        target.next_seq = self.next_seq;
        target.watermark = self.watermark;
        target.late_drops = self.late_drops;
        target.cleanup_state = self.cleanup_state.clone();
        target.range_timers = self.range_timers.clone();
        target.proc_rows = self.proc_rows.clone();
        target.event_cleanup_unarmed = self.event_cleanup_unarmed.clone();
        target.last_write_ms = self.last_write_ms.clone();
        target.pending_rows = self.pending_rows.clone();
        target.last_sweep_ms = self.last_sweep_ms;
        target.retention_bytes = self.retention_bytes;
        let key_types = match &self.inner {
            OverInner::Aggregates(inner) => &inner.key_types,
            OverInner::Bounded(inner) => &inner.key_types,
            OverInner::WindowFunctions(inner) => &inner.key_types,
        };
        target.key_converter = self.key_converter.as_ref().map(|_| {
            let types = self.input_schema.as_ref().map(|schema| {
                self.key_columns
                    .iter()
                    .map(|&column| schema.field(column).data_type().clone())
                    .collect::<Vec<_>>()
            });
            key_row_converter_from_types(types.as_deref().unwrap_or(key_types))
        });
        target.frame_clock = self.frame_clock;
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copyOverCanonicalMemoryState<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    source: jlong,
    target: jlong,
) {
    crate::state::canonical_memory::copy::<OverWindowAggregator>(env, source, target);
}
