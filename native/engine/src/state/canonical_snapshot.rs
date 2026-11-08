use crate::*;
use jni::objects::JObject;

pub(crate) trait CanonicalCursor: Send {
    fn next(&mut self, env: &mut JNIEnv, receiver: &JObject) -> Result<bool, DataFusionError>;
}

pub(crate) struct CanonicalCursorHandle(pub(crate) Box<dyn CanonicalCursor>);

pub(crate) fn cursor<C, F>(snapshot: RocksCanonicalSnapshot<C>, encode: F) -> jlong
where
    C: RocksStateCodec + Send + 'static,
    C::Value: Send,
    F: FnMut(&mut JNIEnv, &JObject, &[(ByteKey, C::Value)]) -> Result<(), DataFusionError>
        + Send
        + 'static,
{
    struct Rows<C: RocksStateCodec, F> {
        snapshot: RocksCanonicalSnapshot<C>,
        encode: F,
    }
    impl<C, F> CanonicalCursor for Rows<C, F>
    where
        C: RocksStateCodec + Send + 'static,
        C::Value: Send,
        F: FnMut(&mut JNIEnv, &JObject, &[(ByteKey, C::Value)]) -> Result<(), DataFusionError>
            + Send
            + 'static,
    {
        fn next(&mut self, env: &mut JNIEnv, receiver: &JObject) -> Result<bool, DataFusionError> {
            let rows = self.snapshot.next_batch()?;
            if rows.is_empty() {
                return Ok(false);
            }
            (self.encode)(env, receiver, &rows)?;
            Ok(true)
        }
    }
    Box::into_raw(Box::new(CanonicalCursorHandle(Box::new(Rows {
        snapshot,
        encode,
    })))) as jlong
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextCanonicalSnapshot<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) -> jni::sys::jboolean {
    crate::bridge::jni_guard(env, move |mut env| {
        let cursor = unsafe { &mut *(handle as *mut CanonicalCursorHandle) };
        match cursor.0.next(&mut env, &receiver) {
            Ok(more) => u8::from(more),
            Err(error) => {
                let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
                0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeCanonicalSnapshot<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        if handle != 0 {
            drop(unsafe { Box::from_raw(handle as *mut CanonicalCursorHandle) });
        }
    });
}
