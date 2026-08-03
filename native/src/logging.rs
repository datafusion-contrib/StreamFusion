use crate::*;

/// Bridges the Rust `log` facade into the host's SLF4J configuration. Each StreamFusion library
/// (the core and every connector/format extension carries its own copy of the `log` statics)
/// installs this bridge from its `JNI_OnLoad`, so everything the native side logs — including
/// librdkafka's log/error stream, which rust-rdkafka's client contexts forward into the facade —
/// lands in the same log files as the host's own logging instead of a void.
///
/// The class and method handles are resolved once at load time, while the loader that owns the
/// StreamFusion classes is in scope: log events fire on arbitrary threads (librdkafka's broker
/// threads are not JVM threads and are attached as daemons on first use), where `FindClass` would
/// resolve against the wrong class loader.
struct LogBridge {
    vm: jni::JavaVM,
    class: jni::objects::GlobalRef,
    method: jni::objects::JStaticMethodID,
}

static BRIDGE: OnceLock<LogBridge> = OnceLock::new();
static LOGGER: NativeLogger = NativeLogger;

struct NativeLogger;

impl log::Log for NativeLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    /// Never panics and never leaves a pending JVM exception: a log call can sit inside a C
    /// callback frame (librdkafka) where unwinding would abort the process.
    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| forward(record)));
    }

    fn flush(&self) {}
}

fn forward(record: &log::Record) {
    let logger = slf4j_logger_name(record.target());
    let message = record.args().to_string();
    if !upcall(record.level(), &logger, &message) {
        eprintln!("[streamfusion native] {} {logger}: {message}", record.level());
    }
}

/// SLF4J logger names are dot-separated; Rust log targets default to `::` module paths.
fn slf4j_logger_name(target: &str) -> String {
    target.replace("::", ".")
}

/// Delivers one record to `NativeLogging.log`; false means the caller should use the stderr
/// fallback (no JVM captured — a standalone test binary — or the upcall failed).
fn upcall(level: log::Level, logger: &str, message: &str) -> bool {
    use jni::signature::{Primitive, ReturnType};

    let Some(bridge) = BRIDGE.get() else {
        return false;
    };
    let Ok(mut env) = bridge.vm.attach_current_thread_as_daemon() else {
        return false;
    };
    env.with_local_frame(4, |env| -> Result<bool, jni::errors::Error> {
        let logger = env.new_string(logger)?;
        let message = env.new_string(message)?;
        let class = unsafe { JClass::from_raw(bridge.class.as_obj().as_raw()) };
        unsafe {
            env.call_static_method_unchecked(
                &class,
                bridge.method,
                ReturnType::Primitive(Primitive::Void),
                &[
                    jni::objects::JValue::Int(level as jni::sys::jint).as_jni(),
                    jni::objects::JValue::Object(&logger).as_jni(),
                    jni::objects::JValue::Object(&message).as_jni(),
                ],
            )?;
        }
        if env.exception_check()? {
            env.exception_clear()?;
            return Ok(false);
        }
        Ok(true)
    })
    .unwrap_or(false)
}

/// Runs once per loaded StreamFusion library. A build running without the StreamFusion classes on
/// the class path (the Flink-less extension-JAR probe) skips the install and drops logs; nothing
/// here may leave a pending exception or fail the load.
fn install(vm: *mut jni::sys::JavaVM) {
    let Ok(vm) = (unsafe { jni::JavaVM::from_raw(vm) }) else {
        return;
    };
    let Ok(mut env) = vm.get_env() else {
        return;
    };
    let class = env.find_class("tech/streamfusion/NativeLogging");
    if matches!(env.exception_check(), Ok(true)) {
        let _ = env.exception_clear();
    }
    let Ok(class) = class else {
        return;
    };
    let Ok(method) =
        env.get_static_method_id(&class, "log", "(ILjava/lang/String;Ljava/lang/String;)V")
    else {
        return;
    };
    let Ok(class) = env.new_global_ref(&class) else {
        return;
    };
    if BRIDGE.set(LogBridge { vm, class, method }).is_ok() {
        let _ = log::set_logger(&LOGGER);
        log::set_max_level(configured_level(std::env::var("STREAMFUSION_NATIVE_LOG").ok()));
    }
}

fn configured_level(value: Option<String>) -> log::LevelFilter {
    value
        .and_then(|level| level.parse().ok())
        .unwrap_or(log::LevelFilter::Info)
}

#[no_mangle]
pub extern "system" fn JNI_OnLoad(
    vm: *mut jni::sys::JavaVM,
    _reserved: *mut std::os::raw::c_void,
) -> jni::sys::jint {
    let _ = std::panic::catch_unwind(|| install(vm));
    jni::sys::JNI_VERSION_1_8
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The DSO also runs standalone (cargo test, Criterion): with no JVM captured, delivery must
    /// fall back to stderr instead of panicking or dereferencing a missing bridge.
    #[test]
    fn forwards_without_a_jvm() {
        assert!(BRIDGE.get().is_none());
        forward(
            &log::Record::builder()
                .args(format_args!("no JVM present"))
                .level(log::Level::Warn)
                .target("streamfusion::logging::test")
                .build(),
        );
        assert!(!upcall(log::Level::Warn, "streamfusion.logging.test", "no JVM present"));
    }

    #[test]
    fn logger_names_are_dot_separated() {
        assert_eq!(slf4j_logger_name("streamfusion::kafka"), "streamfusion.kafka");
        assert_eq!(slf4j_logger_name("librdkafka"), "librdkafka");
    }

    #[test]
    fn level_defaults_to_info_and_honors_the_override() {
        assert_eq!(configured_level(None), log::LevelFilter::Info);
        assert_eq!(configured_level(Some("garbage".to_string())), log::LevelFilter::Info);
        assert_eq!(configured_level(Some("debug".to_string())), log::LevelFilter::Debug);
        assert_eq!(configured_level(Some("off".to_string())), log::LevelFilter::Off);
    }
}
