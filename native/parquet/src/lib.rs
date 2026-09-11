#[allow(unused_imports)]
use streamfusion_bridge::prelude::*;
#[allow(unused_imports)]
use streamfusion_bridge::{self as bridge, flink_text, json_string, *};
streamfusion_bridge::link_allocator!();
mod files;
#[allow(unused_imports)]
use files::*;

#[cfg(test)]
mod tests;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_liveNativeHandles<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    streamfusion_bridge::live_handles_probe(env)
}
