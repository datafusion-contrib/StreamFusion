use super::transfer::FrameKind;
use super::*;
use ::jni::objects::JObject;

fn check_handle(handle: jlong) -> Result<(), DataFusionError> {
    if handle == 0 {
        return Err(DataFusionError::Execution(
            "stock extrema require a native state owner".into(),
        ));
    }
    Ok(())
}

macro_rules! owner {
    ($handle:expr, $persistent:expr, |$owner:ident| $body:block) => {{
        if $persistent == 0 {
            let $owner = unsafe { &mut *($handle as *mut GroupAggregator) };
            $body
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                let $owner = unsafe { &mut *($handle as *mut GroupAggregator<RocksGroupStore>) };
                $body
            }
            #[cfg(not(feature = "rocksdb-state"))]
            {
                Err(DataFusionError::Execution(
                    "native RocksDB is unavailable".into(),
                ))
            }
        }
    }};
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
        let result = check_handle(handle).and_then(|_| {
            let expected = read_int_array(&env, &kinds);
            owner!(handle, persistent, |owner| {
                owner.configure_stock_extrema(&expected)
            })
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
        let result = check_handle(handle).and_then(|_| {
            owner!(handle, persistent, |owner| {
                let mut memory = owner.memory.temporary_reservation();
                owner.visit_stock_extrema(|kind, batch| {
                    let receiver = match kind {
                        FrameKind::Headers => &header_receiver,
                        FrameKind::Entries => &entry_receiver,
                    };
                    canonical::emit(&mut env, receiver, batch, &mut memory)
                })
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
        let result = check_handle(handle).and_then(|_| {
            let kind = if entries != 0 {
                FrameKind::Entries
            } else {
                FrameKind::Headers
            };
            owner!(handle, persistent, |owner| {
                owner.transfer_stock_extrema(kind, &batch, validating)
            })
        });
        // A JVM Arrow release callback can clear a pending exception.
        drop(batch);
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}
