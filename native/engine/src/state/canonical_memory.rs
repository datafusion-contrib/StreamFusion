use crate::*;

pub(crate) trait CanonicalMemoryState {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError>;
}

/// Reserve before capture changes either owner. Refusal leaves both tracked sizes intact.
pub(crate) fn reserve_capture(
    source: &mut OperatorMemory,
    target: &mut OperatorMemory,
    captured_bytes: usize,
    source_growth: usize,
) -> Result<(), DataFusionError> {
    let source_before = source.state_bytes;
    if source.tracking() {
        source.set(source_before.saturating_add(source_growth));
        if let Err(error) = source.account() {
            source.set(source_before);
            return Err(error);
        }
    }
    let target_before = target.state_bytes;
    target.set(captured_bytes);
    if let Err(error) = target.account() {
        target.set(target_before);
        if source.tracking() {
            source.set(source_before);
            source.account_shrink();
        }
        return Err(error);
    }
    Ok(())
}

pub(crate) fn copy<T: CanonicalMemoryState>(env: JNIEnv<'_>, source: jlong, target: jlong) {
    crate::bridge::jni_guard(env, move |mut env| {
        assert!(
            source != 0 && target != 0 && source != target,
            "independent memory snapshot handles"
        );
        let source = unsafe { &mut *(source as *mut T) };
        let target = unsafe { &mut *(target as *mut T) };
        if let Err(error) = source.copy_state(target) {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
            }
        }
    });
}
