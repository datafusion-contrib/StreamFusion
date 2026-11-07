use super::transfer::FrameKind;
use super::*;
use ::jni::objects::JObject;

fn check_handle(handle: jlong, persistent: jboolean) -> Result<(), DataFusionError> {
    if handle == 0 || persistent != 0 {
        return Err(DataFusionError::Execution(
            "stock extrema require the memory or raw-keyed RocksDB group state owner".into(),
        ));
    }
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureGroupExtremaCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    kinds: JIntArray<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = check_handle(handle, persistent).and_then(|_| {
            let expected = read_int_array(&env, &kinds);
            unsafe { &mut *(handle as *mut GroupAggregator) }.configure_stock_extrema(&expected)
        });
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportGroupExtremaCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    header_receiver: JObject<'local>,
    entry_receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = check_handle(handle, persistent).and_then(|_| {
            let owner = unsafe { &*(handle as *const GroupAggregator) };
            let mut memory = owner.memory.temporary_reservation();
            owner.visit_stock_extrema(|kind, batch| {
                let receiver = match kind {
                    FrameKind::Headers => &header_receiver,
                    FrameKind::Entries => &entry_receiver,
                };
                canonical::emit(&mut env, receiver, batch, &mut memory)
            })
        });
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_validateGroupExtremaCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    entries: jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, entries, array, schema, true);
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importGroupExtremaCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    entries: jboolean,
    array: jlong,
    schema: jlong,
) {
    transfer(env, handle, persistent, entries, array, schema, false);
}

fn transfer<'local>(
    env: JNIEnv<'local>,
    handle: jlong,
    persistent: jboolean,
    entries: jboolean,
    array: jlong,
    schema: jlong,
    validating: bool,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let batch = import_record_batch(array, schema);
        let result = check_handle(handle, persistent).and_then(|_| {
            let kind = if entries != 0 {
                FrameKind::Entries
            } else {
                FrameKind::Headers
            };
            unsafe { &mut *(handle as *mut GroupAggregator) }
                .transfer_stock_extrema(kind, &batch, validating)
        });
        // A JVM Arrow release callback can clear a pending exception.
        drop(batch);
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}
