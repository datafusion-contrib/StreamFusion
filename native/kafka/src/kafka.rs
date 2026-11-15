use crate::*;
fn kafka_jni<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> Result<T, String>,
{
    connector_jni(env, default, "native Kafka serialization panic", f)
}

/// The codec extension's build stamp for the loader compatibility check.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_nativeBuildVersion<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    crate::bridge::version_probe(env)
}

/// Whether the Kafka serialization extension loaded successfully.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_isLoaded<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jni::sys::jboolean {
    crate::bridge::jni_guard(env, move |_env| true as jni::sys::jboolean)
}

/// Imports a whole Arrow batch once and materializes the final `byte[]` values consumed by
/// The FLOAT/DOUBLE admission probe's data plane: spells every value with the legacy JDK
/// algorithm (`jdk_double`), newline-terminated, doubles before floats. The JVM compares the
/// result against its own `Double.toString`/`Float.toString` over a corpus where the legacy and
/// shortest-representation algorithms are known to disagree, so a JDK 19+ host fails the probe
/// and FLOAT/DOUBLE columns fall back instead of silently diverging.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_spellFloatingPoint<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    doubles: JDoubleArray<'local>,
    floats: JFloatArray<'local>,
) -> jbyteArray {
    kafka_jni(&mut env, std::ptr::null_mut(), |env| {
        let mut out = Vec::new();
        for value in read_doubles(env, &doubles) {
            crate::jdk_double::jdk_double_to_string(value, &mut out);
            out.push(b'\n');
        }
        for value in read_floats(env, &floats) {
            crate::jdk_double::jdk_float_to_string(value, &mut out);
            out.push(b'\n');
        }
        let result = env
            .byte_array_from_slice(&out)
            .map_err(|error| format!("failed to materialize float spellings: {error}"))?;
        Ok(result.into_raw())
    })
}
