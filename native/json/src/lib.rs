#[allow(unused_imports)]
use streamfusion_bridge::prelude::*;
#[allow(unused_imports)]
use streamfusion_bridge::{self as bridge, flink_text, json_string, *};
use streamfusion_format_support::*;
streamfusion_bridge::link_allocator!();
mod formats;
#[allow(unused_imports)]
use formats::*;

streamfusion_format_support::format_jni_facade!(
    Java_tech_streamfusion_format_json_NativeJsonFormat_driverInitAddress,
    Java_tech_streamfusion_format_json_NativeJsonFormat_isLoaded,
    Java_tech_streamfusion_format_json_NativeJsonFormat_nativeBuildVersion,
    Java_tech_streamfusion_format_json_NativeJsonFormat_decodeInto,
    Java_tech_streamfusion_format_json_NativeJsonFormat_closeDecoder
);
mod json;
#[allow(unused_imports)]
use json::*;
mod json_retry;
#[allow(unused_imports)]
use json_retry::*;
pub mod bench;

#[cfg(test)]
mod tests;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_json_NativeJsonFormat_liveNativeHandles<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    streamfusion_bridge::live_handles_probe(env)
}
