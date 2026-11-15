use super::*;

mod range;
use range::RangePhase;

#[derive(Clone, Copy, Default)]
struct Phase {
    stage: u8,
    deadline: Option<i64>,
    last_event: Option<i64>,
    range: Option<RangePhase>,
}

impl Phase {
    fn advance(
        &mut self,
        kind: i8,
        time: i64,
        polling: bool,
        cleaning: bool,
        range: bool,
        ordered: bool,
    ) -> Result<(), DataFusionError> {
        if range {
            return self
                .range
                .get_or_insert_default()
                .advance(kind, time, ordered);
        }
        match kind {
            CLEANUP if cleaning && self.stage == 0 => {
                self.stage = 1;
                self.deadline = Some(time);
            }
            PROCESSING if cleaning && self.stage == 1 && self.deadline == Some(time) => {
                self.stage = 2;
            }
            EVENT if polling && self.last_event.is_none_or(|previous| previous < time) => {
                self.stage = 3;
                self.last_event = Some(time);
            }
            _ => return Err(invalid("callback kind, order or deadline changed")),
        }
        Ok(())
    }
}

pub(in crate::over_agg) struct CallbackImport {
    key: ByteKey,
    phase: Phase,
    failed: bool,
    validating: bool,
}

impl CallbackImport {
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + byte_key_bytes(&self.key.0)
    }
}

struct PreparedCallbacks {
    types: Vec<DataType>,
    converter: Option<RowConverter>,
    keys: arrow::row::Rows,
    kinds: arrow::array::Int8Array,
    times: Int64Array,
    work: Option<MemoryReservation>,
    phase: Phase,
}

impl OverWindowAggregator {
    fn callback_metadata_bytes(&self) -> usize {
        #[cfg(feature = "rocksdb-state")]
        let journal = self.metadata_updates.bytes;
        #[cfg(not(feature = "rocksdb-state"))]
        let journal = 0;
        self.retained_state_bytes() + journal
    }

    // A hot owner's ledger stays incomplete across chunks; account without serializing it again.
    fn account_callback_state(&mut self) -> Result<(), DataFusionError> {
        if self.memory.tracking() {
            let bytes = self.callback_metadata_bytes();
            self.memory.set(bytes);
            self.memory.account()?;
        }
        Ok(())
    }

    fn prepare_callbacks(
        &self,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<PreparedCallbacks, DataFusionError> {
        self.canonical_event()?;
        let range = self.range_offset().is_some();
        if range
            && self
                .canonical_frame_import
                .as_ref()
                .is_some_and(|frame| frame.failed || !frame.awaiting_protocol)
        {
            return Err(invalid("RANGE frame payload is incomplete or failed"));
        }
        if self
            .canonical_callbacks_import
            .as_ref()
            .is_some_and(|state| state.failed)
        {
            return Err(invalid("failed callback restore must be closed"));
        }
        if self
            .canonical_callbacks_import
            .as_ref()
            .is_some_and(|state| state.validating != validating)
        {
            return Err(invalid("callback restore pass changed before completion"));
        }
        check_cancelled()?;
        if range && batch.num_rows() == 0 && self.canonical_callbacks_import.is_none() {
            return Err(invalid("missing RANGE callback owner"));
        }
        let arity = self.key_columns.len();
        if batch.num_columns() != arity + 2
            || batch.schema().field(arity).name() != "__over_callback_kind"
            || batch.schema().field(arity + 1).name() != "__over_callback_time"
        {
            return Err(invalid("callback schema changed"));
        }
        let kinds = batch
            .column(arity)
            .as_any()
            .downcast_ref::<arrow::array::Int8Array>()
            .filter(|values| values.null_count() == 0)
            .ok_or_else(|| invalid("callback kind must be non-NULL TINYINT"))?;
        let times = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .filter(|values| values.null_count() == 0)
            .ok_or_else(|| invalid("callback time must be non-NULL BIGINT"))?;
        let arrays = (0..arity)
            .map(|column| batch.column(column))
            .collect::<Vec<_>>();
        let types = key_types(&arrays);
        let expected = self.cleanup_key_types();
        if !expected.is_empty() && expected != types {
            return Err(invalid("callback key type changed"));
        }
        let mut work = self.memory.temporary_reservation();
        if let Some(work) = &mut work {
            work.try_resize(
                batch.get_array_memory_size() * 3
                    + batch.num_rows() * 64
                    + std::mem::size_of::<CallbackImport>(),
            )?;
        }
        let mut converter = Some(key_row_converter_from_types(&types));
        let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
        let mut active = self
            .canonical_callbacks_import
            .as_ref()
            .map(|state| state.key.0.as_ref());
        let mut phase = self
            .canonical_callbacks_import
            .as_ref()
            .map_or_else(Phase::default, |state| state.phase);
        let mut map_times = ahash::HashSet::default();
        for row in 0..batch.num_rows() {
            let key = keys.row(row);
            if active != Some(key.data()) {
                if range && active.is_some() {
                    return Err(invalid("mixed RANGE callback owner"));
                }
                phase = Phase::default();
                active = Some(key.data());
            }
            phase.advance(
                kinds.value(row),
                times.value(row),
                self.event_polling,
                self.deadline_cleaning(),
                range,
                self.persistent_callbacks(),
            )?;
            if range && kinds.value(row) == OWNER && self.range_timers.contains_key(key.data()) {
                return Err(invalid("duplicate RANGE callback owner"));
            }
            if range && kinds.value(row) == MAP_ORDER && !map_times.insert(times.value(row)) {
                return Err(invalid("duplicate RANGE map timestamp"));
            }
            if kinds.value(row) == EVENT
                && self
                    .range_timers
                    .get(key.data())
                    .and_then(|timers| timers.armed.last())
                    .is_some_and(|previous| *previous >= times.value(row))
            {
                return Err(invalid("duplicate or unordered callback timer"));
            }
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &self.store {
                let columns = (0..arity).collect::<Vec<_>>();
                if !store.owns_group(store.key_group(binary_row_hash(
                    batch,
                    &columns,
                    row,
                    &self.key_timestamp_precisions,
                ))) {
                    return Err(invalid("unowned callback key"));
                }
            }
        }
        Ok(PreparedCallbacks {
            types,
            converter,
            keys,
            kinds: kinds.clone(),
            times: times.clone(),
            work,
            phase,
        })
    }

    fn apply_callbacks(&mut self, mut prepared: PreparedCallbacks) -> Result<(), DataFusionError> {
        let base = prepared.work.as_ref().map_or(0, MemoryReservation::size);
        let mut initial_metadata = self.callback_metadata_bytes();
        let cleaning = self.deadline_cleaning();
        let range = self.range_offset().is_some();
        let ordered = self.persistent_callbacks();
        self.inner.seed_key_codec(&prepared.types);
        self.key_converter = prepared.converter.take();
        for row in 0..prepared.times.len() {
            check_cancelled()?;
            let encoded = prepared.keys.row(row);
            let kind = prepared.kinds.value(row);
            let time = prepared.times.value(row);
            if self
                .canonical_callbacks_import
                .as_ref()
                .is_some_and(|state| state.key.0.as_ref() != encoded.data())
            {
                if let Some(work) = &mut prepared.work {
                    work.resize(base);
                }
                self.account_callback_state()?;
                #[cfg(feature = "rocksdb-state")]
                self.flush_metadata_updates()?;
                initial_metadata = self.callback_metadata_bytes();
            }
            let key_bytes = byte_key_bytes(encoded.data());
            let grow = if kind == EVENT {
                let (length, capacity) = self
                    .range_timers
                    .get(encoded.data())
                    .map_or((0, 0), |timers| {
                        (timers.armed.len(), timers.armed.capacity())
                    });
                if length == capacity {
                    capacity
                        .saturating_mul(2)
                        .max(4)
                        .saturating_mul(std::mem::size_of::<i64>())
                } else {
                    0
                }
            } else {
                self.range_record_growth(encoded.data(), kind)
            };
            if let Some(work) = &mut prepared.work {
                work.try_resize(
                    base + self
                        .callback_metadata_bytes()
                        .saturating_sub(initial_metadata)
                        + key_bytes.saturating_mul(4)
                        + std::mem::size_of::<CallbackImport>()
                        + grow,
                )?;
            }
            let key = ByteKey::from(encoded.data());
            if self
                .canonical_callbacks_import
                .as_ref()
                .is_none_or(|state| state.key != key)
            {
                if let Some(previous) = self.canonical_callbacks_import.take() {
                    self.retention_bytes -= previous.bytes();
                }
                let state = CallbackImport {
                    key: key.clone(),
                    phase: Phase::default(),
                    failed: false,
                    validating: false,
                };
                self.retention_bytes += state.bytes();
                self.canonical_callbacks_import = Some(state);
            }
            self.canonical_callbacks_import
                .as_mut()
                .unwrap()
                .phase
                .advance(kind, time, self.event_polling, cleaning, range, ordered)?;
            match kind {
                CLEANUP => {
                    if self.cleanup_state.insert(key.clone(), time).is_none() {
                        self.retention_bytes += key_bytes;
                    }
                    if !range && self.event_cleanup_unarmed.insert(key.clone()) {
                        self.retention_bytes += key_bytes;
                    }
                }
                PROCESSING => {
                    if self.event_cleanup_unarmed.remove(&key) {
                        self.retention_bytes -= key_bytes;
                    }
                }
                EVENT => {
                    if !self.range_timers.contains_key(&key) {
                        self.retention_bytes += key_bytes + RangeTimers::default().bytes();
                    }
                    let timers = self.range_timers.entry(key.clone()).or_default();
                    let previous = timers.bytes();
                    timers.armed.push(time);
                    self.retention_bytes += timers.bytes() - previous;
                }
                OWNER | LAST_TRIGGER | MAP_ORDER => self.apply_range_record(&key, kind, time)?,
                _ => unreachable!("validated callback kind"),
            }
            self.mark_store_metadata(&key.0);
        }
        Ok(())
    }

    fn validate_callbacks(
        &mut self,
        mut prepared: PreparedCallbacks,
        final_chunk: bool,
    ) -> Result<(), DataFusionError> {
        if final_chunk && self.range_offset().is_some() {
            let key = if prepared.times.is_empty() {
                self.canonical_callbacks_import
                    .as_ref()
                    .map(|state| state.key.0.as_ref())
            } else {
                Some(prepared.keys.row(prepared.times.len() - 1).data())
            };
            if let Some(key) = key {
                self.check_range_callback_end(key, &prepared.phase, true)?;
            }
        }
        let next = if !final_chunk && !prepared.times.is_empty() {
            let key = prepared.keys.row(prepared.times.len() - 1);
            if let Some(work) = &mut prepared.work {
                work.try_resize(
                    work.size()
                        + byte_key_bytes(key.data())
                        + std::mem::size_of::<CallbackImport>(),
                )?;
            }
            Some(CallbackImport {
                key: ByteKey::from(key.data()),
                phase: prepared.phase,
                failed: false,
                validating: true,
            })
        } else if final_chunk {
            None
        } else {
            return Ok(());
        };
        if let Some(previous) = self.canonical_callbacks_import.take() {
            self.retention_bytes -= previous.bytes();
        }
        if let Some(next) = &next {
            self.retention_bytes += next.bytes();
        }
        self.canonical_callbacks_import = next;
        drop(prepared);
        self.account_callback_state()
    }

    pub(in crate::over_agg) fn transfer_canonical_callbacks(
        &mut self,
        batch: &RecordBatch,
        final_chunk: bool,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        let result = self
            .prepare_callbacks(batch, validating)
            .and_then(|prepared| {
                if validating {
                    return self.validate_callbacks(prepared, final_chunk);
                }
                self.apply_callbacks(prepared)?;
                self.account_callback_state()?;
                if final_chunk {
                    if self.range_offset().is_some() {
                        if let Some(state) = &self.canonical_callbacks_import {
                            self.check_range_callback_end(&state.key.0, &state.phase, false)?;
                        }
                    }
                    self.account()?;
                    if self.range_offset().is_some() {
                        self.finish_range_callback_frame();
                    }
                    if let Some(state) = self.canonical_callbacks_import.take() {
                        self.retention_bytes -= state.bytes();
                    }
                    self.account_callback_state()?;
                }
                Ok(())
            });
        if result.is_err() {
            if self.range_offset().is_some() && !validating {
                if let Some(frame) = &mut self.canonical_frame_import {
                    frame.failed = true;
                }
            }
            if let Some(state) = &mut self.canonical_callbacks_import {
                state.failed = true;
                let _ = self.account_callback_state();
            }
        }
        result
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importOverCanonicalCallbacks<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
    final_chunk: jni::sys::jboolean,
    validating: jni::sys::jboolean,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let result = operator.transfer_canonical_callbacks(
            &import_record_batch(array, schema),
            final_chunk != 0,
            validating != 0,
        );
        if let Err(error) = result {
            report(&mut env, error);
        }
    });
}
