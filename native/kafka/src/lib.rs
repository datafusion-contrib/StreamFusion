#[allow(unused_imports)]
use streamfusion_bridge::prelude::*;
#[allow(unused_imports)]
use streamfusion_bridge::{self as bridge, flink_text, json_string, *};
streamfusion_bridge::link_allocator!();
mod kafka;
#[allow(unused_imports)]
use kafka::*;
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_liveNativeHandles<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    streamfusion_bridge::live_handles_probe(env)
}
