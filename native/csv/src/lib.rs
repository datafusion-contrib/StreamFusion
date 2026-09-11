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
    Java_tech_streamfusion_format_csv_NativeCsvFormat_driverInitAddress,
    Java_tech_streamfusion_format_csv_NativeCsvFormat_isLoaded,
    Java_tech_streamfusion_format_csv_NativeCsvFormat_nativeBuildVersion,
    Java_tech_streamfusion_format_csv_NativeCsvFormat_decodeInto,
    Java_tech_streamfusion_format_csv_NativeCsvFormat_closeDecoder
);
mod csv;
#[allow(unused_imports)]
use csv::*;

#[cfg(test)]
mod tests;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_csv_NativeCsvFormat_liveNativeHandles<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    streamfusion_bridge::live_handles_probe(env)
}
