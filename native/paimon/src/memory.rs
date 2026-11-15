use jni::objects::{GlobalRef, JObject, JValue};
use jni::{JNIEnv, JavaVM};
use paimon::resource::MemoryPool;
use std::sync::Arc;

/// Suballocates a compact reader's share of the JVM's prepaid task-off-heap reservation.
pub(crate) struct CompactPool {
    vm: JavaVM,
    memory: GlobalRef,
}

impl std::fmt::Debug for CompactPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PaimonCompactTaskOffHeapPool")
    }
}

impl CompactPool {
    pub(crate) fn new(env: &JNIEnv<'_>, memory: JObject<'_>) -> Option<Arc<dyn MemoryPool>> {
        (!memory.is_null()).then(|| {
            Arc::new(Self {
                vm: env.get_java_vm().expect("Java VM"),
                memory: env.new_global_ref(memory).expect("Paimon compact memory"),
            }) as Arc<dyn MemoryPool>
        })
    }

    fn call(&self, name: &str, signature: &str, bytes: usize) -> paimon::Result<bool> {
        let mut env = self.vm.attach_current_thread().map_err(exhausted)?;
        let result = env.call_method(
            self.memory.as_obj(),
            name,
            signature,
            &[JValue::Long(i64::try_from(bytes).map_err(exhausted)?)],
        );
        if env.exception_check().map_err(exhausted)? {
            // MemoryPool methods cannot panic or leave a pending JVM exception on a worker.
            let _ = env.exception_clear();
            return Err(exhausted("JVM compact memory callback failed"));
        }
        let value = result.map_err(exhausted)?;
        if signature.ends_with('Z') {
            value.z().map_err(exhausted)
        } else {
            Ok(true)
        }
    }
}

fn exhausted(error: impl std::fmt::Display) -> paimon::Error {
    paimon::Error::ResourceExhausted {
        message: format!("Paimon compact task-off-heap reservation: {error}"),
    }
}

impl MemoryPool for CompactPool {
    fn try_reserve(&self, bytes: usize) -> paimon::Result<()> {
        if self.call("tryReserveReader", "(J)Z", bytes)? {
            Ok(())
        } else {
            Err(exhausted(format!("denied {bytes} reader bytes")))
        }
    }

    fn release(&self, bytes: usize) {
        if let Err(error) = self.call("releaseReader", "(J)V", bytes) {
            eprintln!("{error}");
        }
    }
}
