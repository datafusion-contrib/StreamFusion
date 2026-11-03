use jni::objects::JObject;
use streamfusion_bridge::prelude::*;
use streamfusion_bridge::{self as bridge, *};
streamfusion_bridge::link_allocator!();
mod input;
mod reader;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_createReader(
    env: JNIEnv,
    _class: JClass,
    io: JObject,
    descriptor: JString,
    output_schema: jlong,
    rows: jint,
    budget: jlong,
    kinds: jboolean,
) -> jlong {
    bridge::jni_guard(env, |env| {
        let json: String = env
            .get_string(&descriptor)
            .expect("Paimon read descriptor")
            .into();
        assert!(rows > 0 && budget > 0);
        let io = input::file_io(env, io);
        into_handle(reader::Reader::new(
            io,
            &json,
            import_schema(output_schema),
            rows as usize,
            budget as usize,
            kinds != 0,
        ))
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_readerNext(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jboolean {
    bridge::jni_guard(env, |_| {
        let reader = unsafe { &mut *(handle as *mut reader::Reader) };
        match reader.next() {
            Some(batch) => {
                export_record_batch(batch, array, schema);
                1
            }
            None => 0,
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_readerReservedBytes(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    bridge::jni_guard(env, |_| unsafe {
        (*(handle as *mut reader::Reader)).reserved() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_closeReader(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| unsafe {
        drop(from_handle::<reader::Reader>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_nativeBuildVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::version_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_paimon_NativePaimon_liveNativeHandles(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    bridge::live_handles_probe(env)
}
