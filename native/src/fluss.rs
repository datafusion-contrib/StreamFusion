use crate::*;

/// Feature probe for the JVM planner. The actual fluss-rs reader lands in a later slice once
/// StreamFusion and fluss-rs are on the same Arrow version.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flussFeatureBuilt<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    false as jboolean
}
