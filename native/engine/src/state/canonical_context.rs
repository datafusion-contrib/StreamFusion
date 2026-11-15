use crate::*;
use jni::objects::JObject;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) type CapturedView = Arc<dyn Any + Send + Sync>;

/// Owns immutable state references and cancellation independently of a running operator.
pub(crate) struct CanonicalContext {
    views: HashMap<PathBuf, CapturedView>,
    cancelled: AtomicBool,
    memory: OperatorMemory,
}

thread_local! { static CURRENT: RefCell<Option<Arc<CanonicalContext>>> = const { RefCell::new(None) }; }

impl CanonicalContext {
    #[cfg(test)]
    pub(crate) fn new(views: HashMap<PathBuf, CapturedView>) -> Arc<Self> {
        Self::with_memory(views, OperatorMemory::unaccounted())
    }

    pub(crate) fn with_memory(
        views: HashMap<PathBuf, CapturedView>,
        memory: OperatorMemory,
    ) -> Arc<Self> {
        Arc::new(Self {
            views,
            cancelled: AtomicBool::new(false),
            memory,
        })
    }

    pub(crate) fn with_budget(
        views: HashMap<PathBuf, CapturedView>,
        budget: i64,
    ) -> Result<Arc<Self>, DataFusionError> {
        let mut memory = OperatorMemory::unaccounted();
        memory.attach("canonical-conversion", budget, 0)?;
        Ok(Self::with_memory(views, memory))
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
    pub(crate) fn with<T>(self: &Arc<Self>, operation: impl FnOnce() -> T) -> T {
        struct Restore(Option<Arc<CanonicalContext>>);
        impl Drop for Restore {
            fn drop(&mut self) {
                CURRENT.with(|scope| {
                    scope.replace(self.0.take());
                });
            }
        }
        let guard = Restore(CURRENT.with(|scope| scope.replace(Some(Arc::clone(self)))));
        let result = operation();
        drop(guard);
        result
    }
}

pub(crate) fn in_frozen_scope() -> bool {
    CURRENT.with(|scope| scope.borrow().is_some())
}

pub(crate) fn view(path: &Path) -> Result<Option<CapturedView>, DataFusionError> {
    CURRENT.with(|scope| {
        let scope = scope.borrow();
        let Some(scope) = scope.as_ref() else {
            return Ok(None);
        };
        scope.views.get(path).cloned().map(Some).ok_or_else(|| {
            DataFusionError::Execution("canonical constructor opened an uncaptured database".into())
        })
    })
}

pub(crate) fn temporary_reservation() -> Option<MemoryReservation> {
    CURRENT.with(|scope| {
        scope
            .borrow()
            .as_ref()
            .and_then(|scope| scope.memory.temporary_reservation())
    })
}

pub(crate) fn check_cancelled() -> Result<(), DataFusionError> {
    CURRENT.with(|scope| {
        if scope
            .borrow()
            .as_ref()
            .is_some_and(|scope| scope.cancelled.load(Ordering::Acquire))
        {
            Err(DataFusionError::Execution(
                "canonical conversion was cancelled".into(),
            ))
        } else {
            Ok(())
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createCanonicalMemoryContext<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    memory_budget: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        match CanonicalContext::with_budget(HashMap::new(), memory_budget) {
            Ok(context) => Box::into_raw(Box::new(context)) as jlong,
            Err(error) => {
                throw_memory_limit(&mut env, &error.to_string());
                0
            }
        }
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_withCanonicalDatabases<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    factory: JObject<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let frozen = unsafe { &*(handle as *const std::sync::Arc<CanonicalContext>) };
        frozen.with(|| {
            if let Err(error) = check_cancelled() {
                let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
                return 0;
            }
            match env.call_method(&factory, "getAsLong", "()J", &[]) {
                Ok(value) => value.j().expect("canonical constructor result"),
                Err(_) => 0,
            }
        })
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeCanonicalDatabases<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        if handle != 0 {
            drop(unsafe { Box::from_raw(handle as *mut std::sync::Arc<CanonicalContext>) });
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_cancelCanonicalDatabases<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        if handle != 0 {
            unsafe { &*(handle as *const std::sync::Arc<CanonicalContext>) }.cancel();
        }
    });
}
