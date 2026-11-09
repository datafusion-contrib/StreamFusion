use super::*;

/// Rebound from the stock UNION watermark independently of the native firing frontier.
pub(super) struct SessionAdmission {
    pub(super) watermark: i64,
    calendar: WindowCalendar,
    epoch_bounds: bool,
    event_time: bool,
}

impl SessionAdmission {
    pub(super) fn reject_merge(&self, start: i64, end: i64) {
        if self.epoch_bounds {
            self.calendar
                .reject_expired_epoch_merge(start, end, self.watermark);
        } else {
            self.calendar
                .reject_expired_merge(start, end, self.watermark);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureSessionCanonicalAdmission<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark: jlong,
    callback: jni::objects::JObject<'local>,
    epoch_bounds: jboolean,
) {
    crate::bridge::jni_guard(env, move |env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        aggregator.admission = Some(SessionAdmission {
            watermark,
            calendar: WindowCalendar::new(
                env.new_global_ref(callback)
                    .expect("session admission reference"),
            ),
            epoch_bounds: epoch_bounds != 0,
            event_time: true,
        });
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_advanceSessionCanonicalAdmission<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        match aggregator.admission.as_mut() {
            Some(admission) => {
                if admission.event_time {
                    admission.watermark = admission.watermark.max(watermark);
                }
            }
            None => env
                .throw_new(
                    "java/lang/IllegalStateException",
                    "SESSION admission is not configured",
                )
                .expect("session admission failure"),
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureSessionCanonicalProcessingTime<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        match aggregator.admission.as_mut() {
            Some(admission) => {
                admission.event_time = false;
                admission.watermark = i64::MIN;
            }
            None => env
                .throw_new(
                    "java/lang/IllegalStateException",
                    "SESSION admission is not configured",
                )
                .expect("session admission failure"),
        }
    });
}
