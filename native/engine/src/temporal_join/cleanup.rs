use super::*;

impl TemporalJoiner {
    pub(super) fn next_cleanup(&self) -> Option<i64> {
        if !self.cleaning_enabled() {
            return None;
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store.all_deadlines().values().min().copied();
        }
        self.cleanup_state.values().min().copied()
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindTemporalJoinCleanupTimers<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        joiner.external_cleanup_timers = true;
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextTemporalJoinCleanup<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &*(handle as *const TemporalJoiner) };
        let values = joiner.next_cleanup().into_iter().collect::<Vec<_>>();
        let output = env
            .new_long_array(values.len() as i32)
            .expect("temporal cleanup deadline");
        env.set_long_array_region(&output, 0, &values)
            .expect("temporal cleanup deadline");
        output.into_raw()
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_fireTemporalJoinCleanup<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timestamp: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        if let Err(error) = joiner.expire_processing_time(timestamp) {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
            }
        }
    });
}
