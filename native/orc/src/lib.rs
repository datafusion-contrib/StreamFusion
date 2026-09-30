//! Released orc-rust decoding over host-owned I/O and Arrow C Data.
use jni::objects::{GlobalRef, JObject, JValue};
use std::ffi::{c_int, c_void};
use streamfusion_bridge::prelude::*;
use streamfusion_bridge::{self as bridge, *};
streamfusion_bridge::link_allocator!();
mod reader;
#[cfg(feature = "reader-comparison")]
mod reader_comparison;
#[cfg(feature = "writer-comparison")]
mod writer_comparison;

struct HostIo {
    vm: jni::JavaVM,
    stream: GlobalRef,
}
impl HostIo {
    fn new(env: &mut JNIEnv, stream: JObject) -> Self {
        Self {
            vm: env.get_java_vm().expect("JVM"),
            stream: env.new_global_ref(stream).expect("ORC stream"),
        }
    }
}
// Java I/O exceptions remain pending for the enclosing JNI guard to return to the host.
extern "C" fn read_host(
    context: *mut c_void,
    offset: u64,
    length: u64,
    bytes: *mut c_void,
) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> jni::errors::Result<()> {
            let host = unsafe { &*(context as *const HostIo) };
            let mut env = host.vm.get_env()?;
            let mut done = 0;
            while done < length {
                let size = (length - done).min(i32::MAX as u64) as usize;
                env.with_local_frame(4, |env| -> jni::errors::Result<()> {
                    let buffer = unsafe {
                        env.new_direct_byte_buffer(bytes.cast::<u8>().add(done as usize), size)?
                    };
                    env.call_method(
                        host.stream.as_obj(),
                        "readFully",
                        "(JLjava/nio/ByteBuffer;)V",
                        &[
                            JValue::Long((offset + done) as i64),
                            JValue::Object(buffer.as_ref()),
                        ],
                    )?;
                    Ok(())
                })?;
                done += size as u64;
            }
            Ok(())
        },
    ))
    .map_or(-1, |result| if result.is_ok() { 0 } else { -1 })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_nativeBuildVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::version_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_liveNativeHandles(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::live_handles_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_createOrcDecoder<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    input: JObject<'a>,
    length: jlong,
    schema: jlong,
    names: JObjectArray<'a>,
    batch_size: jint,
    instant_zone: JString<'a>,
) -> jlong {
    bridge::jni_guard(env, |env| {
        assert!(length >= 0 && batch_size > 0);
        let schema = import_schema(schema);
        let names = read_strings(env, &names)
            .into_iter()
            .map(|n| n.expect("null ORC name"))
            .collect();
        let zone: String = env
            .get_string(&instant_zone)
            .expect("ORC instant timezone")
            .into();
        into_handle(reader::Decoder::open(
            HostIo::new(env, input),
            length as u64,
            schema,
            names,
            batch_size as usize,
            &zone,
        ))
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_orcDecoderNext(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jboolean {
    bridge::jni_guard(env, |_| {
        let decoder = unsafe { &mut *(handle as *mut reader::Decoder) };
        let Some(batch) = decoder.next() else {
            return 0;
        };
        export_record_batch(batch, array, schema);
        1
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_orcDecoderMaxStripeBytes(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    bridge::jni_guard(env, |_| {
        let decoder = unsafe { &*(handle as *const reader::Decoder) };
        decoder.memory
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_orc_NativeOrc_closeOrcDecoder(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| unsafe {
        drop(from_handle::<reader::Decoder>(handle));
    })
}

pub mod bench;
