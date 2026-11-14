use crate::*;

/// A codec object is created, invoked and dropped inside its owning format library.
pub trait MessageEncoder {
    fn encode(
        &self,
        batch: &RecordBatch,
        kinds: Option<&Int8Array>,
        types: &[String],
        names: &[String],
    ) -> Result<EncodedLines, String>;
}

pub struct MessageEncoderHandle {
    encoder: Box<dyn MessageEncoder>,
    fields: Vec<usize>,
    types: Vec<String>,
    names: Vec<String>,
}

pub fn create_message_encoder(
    env: &mut JNIEnv,
    format: jint,
    options: &JString,
    types: &JObjectArray,
    names: &JObjectArray,
    fields: &JIntArray,
    create: impl FnOnce(i32, &str) -> Result<Box<dyn MessageEncoder>, String>,
) -> Result<jlong, String> {
    let options: String = env.get_string(options).map_err(|e| e.to_string())?.into();
    let types = read_string_array(env, types);
    let names = read_string_array(env, names);
    let fields = read_int_array(env, fields)
        .into_iter()
        .map(|index| usize::try_from(index).map_err(|_| "negative encoder projection".to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let project = |values: &[String]| {
        fields
            .iter()
            .map(|index| {
                values.get(*index).cloned().ok_or_else(|| {
                    format!("encoder projection {index} is outside its logical schema")
                })
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let types = if types.is_empty() {
        Vec::new()
    } else {
        project(&types)?
    };
    let names = if names.is_empty() {
        Vec::new()
    } else {
        project(&names)?
    };
    Ok(into_handle(MessageEncoderHandle {
        encoder: create(format, &options)?,
        fields,
        types,
        names,
    }))
}

pub fn encode_message_batch(
    env: &mut JNIEnv,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> Result<::jni::sys::jobject, String> {
    assert_ne!(handle, 0, "encoder is closed");
    let handle = unsafe { &*(handle as *const MessageEncoderHandle) };
    let batch = import_record_batch(array, schema);
    let data = batch.project(&handle.fields).map_err(|e| e.to_string())?;
    let encoded =
        handle
            .encoder
            .encode(&data, row_kind_column(&batch), &handle.types, &handle.names)?;
    if encoded.len() != batch.num_rows() {
        return Err("encoder changed the input row count".into());
    }
    let (offsets, lengths) = encoded.metadata(&[])?;
    let bytes = env
        .byte_array_from_slice(&encoded.bytes)
        .map_err(|e| e.to_string())?;
    let offsets_array = env
        .new_int_array(offsets.len() as i32)
        .map_err(|e| e.to_string())?;
    env.set_int_array_region(&offsets_array, 0, &offsets)
        .map_err(|e| e.to_string())?;
    let lengths_array = env
        .new_int_array(lengths.len() as i32)
        .map_err(|e| e.to_string())?;
    env.set_int_array_region(&lengths_array, 0, &lengths)
        .map_err(|e| e.to_string())?;
    env.new_object(
        "tech/streamfusion/format/EncodedMessages",
        "([B[I[I)V",
        &[
            ::jni::objects::JValue::Object(&bytes),
            ::jni::objects::JValue::Object(&offsets_array),
            ::jni::objects::JValue::Object(&lengths_array),
        ],
    )
    .map(|value| value.into_raw())
    .map_err(|e| e.to_string())
}

pub fn spell_floating_point(
    env: &mut JNIEnv,
    doubles: &JDoubleArray,
    floats: &JFloatArray,
) -> Result<jbyteArray, String> {
    let mut out = Vec::new();
    for value in read_doubles(env, doubles) {
        jdk_double::jdk_double_to_string(value, &mut out);
        out.push(b'\n');
    }
    for value in read_floats(env, floats) {
        jdk_double::jdk_float_to_string(value, &mut out);
        out.push(b'\n');
    }
    env.byte_array_from_slice(&out)
        .map(|value| value.into_raw())
        .map_err(|e| e.to_string())
}

#[macro_export]
macro_rules! format_encoder_jni_facade {
    ($create:ident, $encode:ident, $close:ident, $supported:ident, $spell:ident, $factory:path, $supports:path) => {
        #[no_mangle]
        pub extern "system" fn $create(
            mut env: JNIEnv,
            _: JClass,
            format: jint,
            options: JString,
            types: JObjectArray,
            names: JObjectArray,
            fields: JIntArray,
        ) -> jlong {
            streamfusion_bridge::connector_jni(&mut env, 0, "format encoder", |env| {
                $crate::create_message_encoder(
                    env, format, &options, &types, &names, &fields, $factory,
                )
            })
        }
        #[no_mangle]
        pub extern "system" fn $encode(
            mut env: JNIEnv,
            _: JClass,
            handle: jlong,
            array: jlong,
            schema: jlong,
        ) -> ::jni::sys::jobject {
            streamfusion_bridge::connector_jni(
                &mut env,
                std::ptr::null_mut(),
                "format encoder",
                |env| $crate::encode_message_batch(env, handle, array, schema),
            )
        }
        #[no_mangle]
        pub extern "system" fn $close(env: JNIEnv, _: JClass, handle: jlong) {
            streamfusion_bridge::jni_guard(env, |_| {
                assert_ne!(handle, 0, "encoder is closed");
                unsafe {
                    drop(streamfusion_bridge::from_handle::<
                        $crate::MessageEncoderHandle,
                    >(handle));
                }
            })
        }
        #[no_mangle]
        pub extern "system" fn $supported(env: JNIEnv, _: JClass, format: jint) -> jboolean {
            streamfusion_bridge::jni_guard(env, |_| u8::from($supports(format)))
        }
        #[no_mangle]
        pub extern "system" fn $spell(
            mut env: JNIEnv,
            _: JClass,
            doubles: JDoubleArray,
            floats: JFloatArray,
        ) -> jbyteArray {
            streamfusion_bridge::connector_jni(
                &mut env,
                std::ptr::null_mut(),
                "format float spelling",
                |env| $crate::spell_floating_point(env, &doubles, &floats),
            )
        }
    };
}
