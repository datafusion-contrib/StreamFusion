use crate::*;

pub(crate) trait CanonicalMemoryState {
    fn copy_state(&mut self, target: &mut Self) -> Result<(), DataFusionError>;
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
            let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
        }
    });
}
