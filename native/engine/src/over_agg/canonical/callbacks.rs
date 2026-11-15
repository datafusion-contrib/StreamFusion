use super::*;
use crate::state::canonical_context::check_cancelled;

const CLEANUP: i8 = 0;
const LAST_TRIGGER: i8 = 1;
const EVENT: i8 = 2;
const PROCESSING: i8 = 3;
const OWNER: i8 = 4;
const MAP_ORDER: i8 = 5;

use crate::state::canonical_records::KeyedLongWriter as CallbackWriter;
const FIELDS: [&str; 2] = ["__over_callback_kind", "__over_callback_time"];

impl<F: FnMut(RecordBatch) -> Result<(), DataFusionError>> CallbackWriter<F> {
    fn stamp(
        &mut self,
        key: &[u8],
        stamp: Option<(i64, bool)>,
        range: bool,
    ) -> Result<(), DataFusionError> {
        if let Some((deadline, armed)) = stamp {
            self.push(key, CLEANUP, deadline)?;
            if armed && !range {
                self.push(key, PROCESSING, deadline)?;
            }
        }
        Ok(())
    }
}

impl OverWindowAggregator {
    fn visit_canonical_callbacks(
        &self,
        accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.canonical_event()?;
        self.ensure_canonical_ready()?;
        check_cancelled()?;
        let range = self.range_offset().is_some();
        #[cfg(feature = "rocksdb-state")]
        let frozen = self.frozen_store().is_some();
        #[cfg(not(feature = "rocksdb-state"))]
        let frozen = false;
        if !frozen && self.cleanup_state.is_empty() && self.range_timers.is_empty() {
            return Ok(());
        }
        let mut writer = CallbackWriter::new(
            self.event_key_types()?,
            FIELDS,
            self.memory.temporary_reservation(),
            accept,
        );
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = self.frozen_store() {
            if range || self.event_polling {
                store.visit_range_timers_charged(
                    CHUNK_ROWS,
                    self.memory.temporary_reservation(),
                    |key, bytes| {
                        let timers = crate::over_agg::range_timers::RangeTimerView::new(bytes)?;
                        writer.stamp(
                            key,
                            store.cleanup_stamp(self.store_fold_key_group(key), key)?,
                            range,
                        )?;
                        if range {
                            if let Some(last) = timers.last {
                                writer.push(key, LAST_TRIGGER, last)?;
                            }
                        }
                        for time in timers.armed() {
                            writer.push(key, EVENT, time?)?;
                        }
                        Ok(())
                    },
                )?;
            }
            if !range {
                store.visit_cleanup_stamps_charged(
                    CHUNK_ROWS,
                    self.memory.temporary_reservation(),
                    |rows| {
                        for (key, deadline, armed) in rows {
                            if !self.event_polling
                                || !store
                                    .has_range_timer_state(self.store_fold_key_group(&key), &key)?
                            {
                                writer.stamp(&key, Some((deadline, armed)), false)?;
                            }
                        }
                        Ok(())
                    },
                )?;
            }
            return writer.flush();
        }
        if range || self.event_polling {
            for (key, timers) in &self.range_timers {
                let stamp = self
                    .cleanup_state
                    .get(key)
                    .map(|&stamp| (stamp, !self.event_cleanup_unarmed.contains(key)));
                writer.stamp(&key.0, stamp, range)?;
                if range {
                    if let Some(last) = timers.last {
                        writer.push(&key.0, LAST_TRIGGER, last)?;
                    }
                }
                for &time in &timers.armed {
                    writer.push(&key.0, EVENT, time)?;
                }
            }
        }
        if !range {
            for (key, &stamp) in &self.cleanup_state {
                if !self.event_polling || !self.range_timers.contains_key(key) {
                    writer.stamp(
                        &key.0,
                        Some((stamp, !self.event_cleanup_unarmed.contains(key))),
                        false,
                    )?;
                }
            }
        }
        writer.flush()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverCanonicalCallbacks<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        if let Err(error) = operator.visit_canonical_callbacks(|batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            report(&mut env, error);
        }
    });
}

#[cfg(test)]
mod tests;

mod import;
pub(in crate::over_agg) use import::CallbackImport;
