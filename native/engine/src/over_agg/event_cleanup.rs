use super::*;

pub(super) const EVENT_CLEANUP_ARMED: &str = "__sf_over_cleanup_armed";

impl OverWindowAggregator {
    pub(super) fn bind_event_cleanup_timers(&mut self) -> Result<(), DataFusionError> {
        if self.proctime
            || !matches!(self.inner, OverInner::Aggregates(_))
            || !self.deadline_cleaning()
        {
            return Err(DataFusionError::Execution(
                "event cleanup requires enabled unbounded rowtime OVER".into(),
            ));
        }
        self.max_retention_ms = self.min_retention_ms.wrapping_mul(3) / 2;
        self.event_cleanup_external_timers = true;
        Ok(())
    }

    pub(super) fn next_event_cleanup(&self) -> Result<Option<i64>, DataFusionError> {
        if !self.event_cleanup_external_timers {
            return Err(DataFusionError::Execution(
                "event cleanup timers are not bound".into(),
            ));
        }
        Ok(self
            .cleanup_state
            .iter()
            .filter(|(key, _)| !self.event_cleanup_unarmed.contains(*key))
            .map(|(_, &stamp)| stamp)
            .min())
    }

    pub(super) fn fire_event_cleanup(
        &mut self,
        timestamp: i64,
        now: i64,
    ) -> Result<(), DataFusionError> {
        self.next_event_cleanup()?;
        if now < timestamp {
            return Err(DataFusionError::Execution(
                "event cleanup fired before its deadline".into(),
            ));
        }
        let due = self
            .cleanup_state
            .iter()
            .filter(|(key, deadline)| {
                **deadline <= timestamp && !self.event_cleanup_unarmed.contains(*key)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in due {
            if self.pending_rows.contains_key(&key) {
                if self.event_cleanup_unarmed.insert(key.clone()) {
                    self.retention_bytes += byte_key_bytes(&key.0);
                }
                self.register_cleanup(&key.0, now);
            } else {
                self.clear_key(&key.0);
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.flush_tombstones()?;
        }
        self.account()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindOverEventCleanupTimers<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result =
            unsafe { &mut *(handle as *mut OverWindowAggregator) }.bind_event_cleanup_timers();
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextOverEventCleanup<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        match operator.next_event_cleanup() {
            Ok(deadline) => {
                let values = deadline.into_iter().collect::<Vec<_>>();
                let output = env
                    .new_long_array(values.len() as i32)
                    .expect("event cleanup deadline");
                env.set_long_array_region(&output, 0, &values)
                    .expect("event cleanup deadline value");
                output.into_raw()
            }
            Err(error) => {
                let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_fireOverEventCleanup<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timestamp: jlong,
    now: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = unsafe { &mut *(handle as *mut OverWindowAggregator) }
            .fire_event_cleanup(timestamp, now);
        if let Err(error) = result {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
            }
        }
    });
}
