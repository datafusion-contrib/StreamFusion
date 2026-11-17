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
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_driverInitAddress,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_isLoaded,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_nativeBuildVersion,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_decodeInto,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_decodeRecordsInto,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_closeDecoder
);
mod protobuf_decode;

#[cfg(test)]
mod tests;

pub use formats::ProtobufDecoder;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_liveNativeHandles<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    streamfusion_bridge::live_handles_probe(env)
}

pub mod encode;
streamfusion_format_support::format_encoder_jni_facade!(
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_createEncoder,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_encodeBatch,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_closeEncoder,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_encoderSupported,
    Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_spellFloatingPoint,
    encode::create_encoder,
    encode::supports_encoder
);

#[allow(unused_imports)]
use encode::*;

pub use encode::ProtobufEncoder;
