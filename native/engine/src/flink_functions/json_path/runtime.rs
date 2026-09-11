use super::Reader;
use crate::bridge::JVM;
use datafusion::common::{exec_datafusion_err, Result};
use jni::objects::JValue;

/// Hold Jackson's recycled input buffer for a batch, then publish the capacity reached by
/// the native reader. No document or result crosses JNI; both parsers share the same state.
pub(crate) fn with_reader<T>(evaluate: impl FnOnce(&mut Reader) -> Result<T>) -> Result<T> {
    // Standalone Rust tests supply the same initial state as a fresh Jackson recycler.
    #[cfg(test)]
    if JVM.get().is_none() {
        return evaluate(&mut Reader::new(4000));
    }

    let vm = JVM
        .get()
        .ok_or_else(|| exec_datafusion_err!("JVM not captured for SQL/JSON runtime"))?;
    let mut env = vm
        .attach_current_thread()
        .map_err(|e| exec_datafusion_err!("SQL/JSON runtime: {e}"))?;
    env.with_local_frame(4, |env| -> jni::errors::Result<_> {
        let runtime = env.new_object("tech/streamfusion/operator/NativeJsonRuntime", "()V", &[])?;
        let size = env.call_method(&runtime, "bufferSize", "()I", &[])?.i()?;
        let mut reader = Reader::new(size as usize);
        let result = evaluate(&mut reader);
        // Publish growth even if an ON ERROR/EMPTY policy fails the expression.
        env.call_method(
            &runtime,
            "release",
            "(I)V",
            &[JValue::Int(reader.buffer_size() as i32)],
        )?;
        Ok(result)
    })
    .map_err(|e| exec_datafusion_err!("SQL/JSON runtime: {e}"))?
}
