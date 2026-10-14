use jni::objects::{GlobalRef, JLongArray, JValue};
use streamfusion_bridge::bridge::{JavaException, JVM};

/// Batched released-Flink timer conversion; local bounds remain in the Arrow state codec.
pub(crate) struct WindowCalendar {
    callback: GlobalRef,
}

impl WindowCalendar {
    pub(crate) fn new(callback: GlobalRef) -> Self {
        Self { callback }
    }

    pub(crate) fn reject_expired_merge(&self, start: i64, end: i64, clock: i64) {
        let mut env = JVM
            .get()
            .expect("window calendar JVM")
            .attach_current_thread()
            .expect("window calendar thread");
        let result = env
            .with_local_frame(2, |env| -> jni::errors::Result<Result<(), JavaException>> {
                let called = env.call_method(
                    self.callback.as_obj(),
                    "rejectExpiredMerge",
                    "(JJJ)V",
                    &[JValue::Long(start), JValue::Long(end), JValue::Long(clock)],
                );
                if matches!(called, Err(jni::errors::Error::JavaException)) {
                    let throwable = env.exception_occurred()?;
                    env.exception_clear()?;
                    return Ok(Err(JavaException(env.new_global_ref(throwable)?)));
                }
                called?;
                Ok(Ok(()))
            })
            .expect("window merge check");
        result.unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error)))
    }

    pub(crate) fn end_thresholds(
        &self,
        ends: impl IntoIterator<Item = i64>,
    ) -> std::collections::BTreeMap<i64, i64> {
        let ends = ends
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        ends.iter()
            .copied()
            .zip(self.exclusive_ends(&ends))
            .collect()
    }

    pub(crate) fn local_millis(&self, epochs: &[i64]) -> Vec<i64> {
        self.convert("localAssignmentMillis", epochs)
    }

    pub(crate) fn epoch_millis(&self, local: &[i64]) -> Vec<i64> {
        self.convert("epochWindowMillis", local)
    }

    pub(crate) fn exclusive_ends(&self, ends: &[i64]) -> Vec<i64> {
        self.convert("exclusiveTimerEnds", ends)
    }

    fn convert(&self, method: &str, ends: &[i64]) -> Vec<i64> {
        if ends.is_empty() {
            return Vec::new();
        }
        let mut env = JVM
            .get()
            .expect("window calendar JVM")
            .attach_current_thread()
            .expect("window calendar thread");
        let result = env
            .with_local_frame(
                4,
                |env| -> jni::errors::Result<Result<Vec<i64>, JavaException>> {
                    let input = env.new_long_array(ends.len() as i32)?;
                    env.set_long_array_region(&input, 0, ends)?;
                    let called = env.call_method(
                        self.callback.as_obj(),
                        method,
                        "([J)[J",
                        &[JValue::Object(&input)],
                    );
                    if matches!(called, Err(jni::errors::Error::JavaException)) {
                        let throwable = env.exception_occurred()?;
                        env.exception_clear()?;
                        return Ok(Err(JavaException(env.new_global_ref(throwable)?)));
                    }
                    let output = JLongArray::from(called?.l()?);
                    assert_eq!(env.get_array_length(&output)? as usize, ends.len());
                    let mut converted = vec![0; ends.len()];
                    env.get_long_array_region(&output, 0, &mut converted)?;
                    Ok(Ok(converted))
                },
            )
            .expect("window calendar conversion");
        result.unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error)))
    }
}
