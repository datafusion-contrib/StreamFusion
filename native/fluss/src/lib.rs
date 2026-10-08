use streamfusion_bridge::{self as bridge, prelude::*, *};
streamfusion_bridge::link_allocator!();
pub mod write;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_fluss_NativeFluss_nativeBuildVersion(
    env: JNIEnv,
    _: JClass,
) -> jstring {
    bridge::version_probe(env)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_fluss_NativeFluss_liveNativeHandles(
    env: JNIEnv,
    _: JClass,
) -> jstring {
    bridge::live_handles_probe(env)
}
struct BucketSplit {
    groups: Vec<(i32, RecordBatch)>,
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_fluss_NativeFluss_splitByBucket<'a>(
    env: JNIEnv<'a>,
    _: JClass<'a>,
    array: jlong,
    schema: jlong,
    columns: JIntArray<'a>,
    precisions: JIntArray<'a>,
    count: jint,
) -> jlong {
    bridge::jni_guard(env, |env| {
        let batch = import_record_batch(array, schema);
        let columns = bridge::read_columns(&env, &columns);
        let precisions = bridge::read_columns(&env, &precisions);
        let groups = write::split_by_bucket(&batch, &columns, &precisions, count)
            .expect("Fluss bucket routing failed");
        into_handle(BucketSplit {
            groups: groups.into_iter().rev().collect(),
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_fluss_NativeFluss_nextBucketSlice(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jint {
    bridge::jni_guard(env, |_| {
        let split = unsafe { &mut *(handle as *mut BucketSplit) };
        match split.groups.pop() {
            Some((bucket, batch)) => {
                export_record_batch(batch, array, schema);
                bucket
            }
            None => -1,
        }
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_fluss_NativeFluss_closeBucketSplit(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
) {
    bridge::jni_guard(env, |_| unsafe {
        drop(from_handle::<BucketSplit>(handle));
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_fluss_NativeFluss_statistics<'a>(
    env: JNIEnv<'a>,
    _: JClass<'a>,
    array: jlong,
    schema: jlong,
    columns: JIntArray<'a>,
) -> jni::sys::jintArray {
    bridge::jni_guard(env, |env| {
        let batch = import_record_batch(array, schema);
        let columns = bridge::read_columns(&env, &columns);
        let indices = write::statistics(&batch, &columns).expect("Fluss statistics failed");
        let result = env.new_int_array(indices.len() as i32).unwrap();
        env.set_int_array_region(&result, 0, &indices).unwrap();
        result.into_raw()
    })
}
