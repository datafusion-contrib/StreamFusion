#[macro_export]
macro_rules! format_jni_facade {
    (
        $driver_init_address:ident,
        $is_loaded:ident,
        $native_build_version:ident,
        $decode_into:ident,
        $close_decoder:ident
    ) => {
        #[no_mangle]
        pub extern "system" fn $driver_init_address<'local>(
            env: JNIEnv<'local>,
            _class: JClass<'local>,
        ) -> jlong {
            streamfusion_bridge::jni_guard(env, move |_env| {
                $crate::streamfusion_format_driver_init as usize as jlong
            })
        }

        #[no_mangle]
        pub extern "system" fn $is_loaded<'local>(
            env: JNIEnv<'local>,
            _class: JClass<'local>,
        ) -> jboolean {
            streamfusion_bridge::jni_guard(env, move |_env| 1)
        }

        #[no_mangle]
        pub extern "system" fn $native_build_version<'local>(
            env: JNIEnv<'local>,
            _class: JClass<'local>,
        ) -> jstring {
            streamfusion_bridge::version_probe(env)
        }

        #[no_mangle]
        pub extern "system" fn $decode_into<'local>(
            env: JNIEnv<'local>,
            class: JClass<'local>,
            handle: jlong,
            in_array: jlong,
            in_schema: jlong,
            out_array: jlong,
            out_schema: jlong,
        ) {
            $crate::decode_into(
                env, class, handle, in_array, in_schema, out_array, out_schema,
            )
        }

        #[no_mangle]
        pub extern "system" fn $close_decoder<'local>(
            env: JNIEnv<'local>,
            class: JClass<'local>,
            handle: jlong,
        ) {
            $crate::close_decoder(env, class, handle)
        }
    };
}
