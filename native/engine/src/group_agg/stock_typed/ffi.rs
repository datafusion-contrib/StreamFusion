use super::*;
use jni::objects::JObject;

#[cfg(feature = "rocksdb-state")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_captureGroupTypedCanonicalSnapshot<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let owner = unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) };
        let result = (|| {
            owner.typed_contract()?;
            let kinds = owner.kinds.clone();
            let types = owner.state_types.clone();
            let ttl = owner.ttl_ms > 0;
            let append_only = owner.stock_append_only == Some(true);
            let mut memory = owner.memory.temporary_reservation();
            let snapshot = owner.store.canonical_snapshot()?;
            Ok::<_, DataFusionError>(crate::state::canonical_snapshot::cursor(
                snapshot,
                move |env, receiver, entries| {
                    let rows: Vec<_> = entries.iter().map(|(key, value)| (key, value)).collect();
                    canonical::emit(
                        env,
                        receiver,
                        headers(&kinds, &types, ttl, append_only, &rows)?,
                        &mut memory,
                    )
                },
            ))
        })();
        match result {
            Ok(handle) => handle,
            Err(error) => {
                canonical::throw_error(&mut env, error);
                0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureGroupTypedGlobalCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = if persistent == 0 {
            unsafe { &mut *(handle as *mut GroupAggregator) }.configure_stock_global_merge()
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) }
                    .configure_stock_global_merge()
            }
            #[cfg(not(feature = "rocksdb-state"))]
            {
                Err(invalid("native RocksDB is unavailable"))
            }
        };
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_configureGroupTypedCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    kinds: JIntArray<'local>,
    append_only: jboolean,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &kinds);
        let result = if persistent == 0 {
            unsafe { &mut *(handle as *mut GroupAggregator) }
                .configure_typed(&kinds, append_only != 0)
        } else {
            #[cfg(feature = "rocksdb-state")]
            {
                unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) }
                    .configure_typed(&kinds, append_only != 0)
            }
            #[cfg(not(feature = "rocksdb-state"))]
            Err(invalid("native RocksDB is unavailable"))
        };
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportGroupTypedCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    slot: jint,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let result = (|| {
            if slot < -1 {
                return Err(invalid("invalid canonical frame selector"));
            }
            let slot = (slot >= 0).then_some(slot as usize);
            if persistent == 0 {
                let owner = unsafe { &*(handle as *mut GroupAggregator) };
                let mut reservation = owner.memory.temporary_reservation();
                owner.visit_typed(slot, |batch| {
                    canonical::emit(&mut env, &receiver, batch, &mut reservation)
                })
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let owner = unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) };
                    let mut reservation = owner.memory.temporary_reservation();
                    owner.visit_typed(slot, |batch| {
                        canonical::emit(&mut env, &receiver, batch, &mut reservation)
                    })
                }
                #[cfg(not(feature = "rocksdb-state"))]
                Err(invalid("native RocksDB is unavailable"))
            }
        })();
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_transferGroupTypedCanonical<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    persistent: jboolean,
    slot: jint,
    array: jlong,
    schema: jlong,
    validating: jboolean,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        // Imported Arrow buffers must be released before a Java exception leaves JNI.
        let result = {
            let batch = import_record_batch(array, schema);
            if slot < -1 {
                Err(invalid("invalid canonical frame selector"))
            } else if persistent == 0 {
                let owner = unsafe { &mut *(handle as *mut GroupAggregator) };
                if slot < 0 {
                    owner.transfer_typed_headers(&batch, validating != 0)
                } else {
                    owner.transfer_typed_entries(slot as usize, &batch, validating != 0)
                }
            } else {
                #[cfg(feature = "rocksdb-state")]
                {
                    let owner = unsafe { &mut *(handle as *mut GroupAggregator<RocksGroupStore>) };
                    if slot < 0 {
                        owner.transfer_typed_headers(&batch, validating != 0)
                    } else {
                        owner.transfer_typed_entries(slot as usize, &batch, validating != 0)
                    }
                }
                #[cfg(not(feature = "rocksdb-state"))]
                Err(invalid("native RocksDB is unavailable"))
            }
        };
        if let Err(error) = result {
            canonical::throw_error(&mut env, error);
        }
    });
}
