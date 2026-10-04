use arrow::datatypes::Schema;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use jni::objects::{JClass, JObject};
use jni::JavaVM;

/// Owns the production decoder handle while the benchmark's JVM stays alive.
pub struct HostDecoder<'a> {
    vm: &'a JavaVM,
    handle: i64,
}

impl<'a> HostDecoder<'a> {
    pub fn new(
        vm: &'a JavaVM,
        input: &JObject,
        length: usize,
        schema: &Schema,
        batch_rows: usize,
    ) -> Self {
        let mut ffi_schema = FFI_ArrowSchema::try_from(schema).expect("export input schema");
        let mut env = vm.get_env().expect("attached benchmark thread");
        let handle = env
            .with_local_frame(16, |env| -> jni::errors::Result<i64> {
                let names = env.new_object_array(
                    schema.fields().len() as i32,
                    "java/lang/String",
                    JObject::null(),
                )?;
                for (index, field) in schema.fields().iter().enumerate() {
                    let name = env.new_string(field.name())?;
                    env.set_object_array_element(&names, index as i32, &name)?;
                }
                let input = env.new_local_ref(input)?;
                Ok(crate::reader::Java_tech_streamfusion_parquet_NativeParquet_createParquetDecoder(
                    vm.get_env()?,
                    JClass::default(),
                    input,
                    i64::try_from(length).expect("file length"),
                    &mut ffi_schema as *mut FFI_ArrowSchema as i64,
                    names,
                    i32::try_from(batch_rows).expect("batch size"),
                ))
            })
            .expect("construct production host decoder");
        check_exception(vm);
        assert_ne!(handle, 0, "production decoder handle");
        Self { vm, handle }
    }

    pub fn next_batch(&mut self) -> Option<RecordBatch> {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        let array_address = &mut array as *mut FFI_ArrowArray as i64;
        let schema_address = &mut schema as *mut FFI_ArrowSchema as i64;
        let present =
            crate::reader::Java_tech_streamfusion_parquet_NativeParquet_parquetDecoderNext(
                self.vm.get_env().expect("attached benchmark thread"),
                JClass::default(),
                self.handle,
                array_address,
                schema_address,
            );
        check_exception(self.vm);
        (present != 0)
            .then(|| streamfusion_bridge::import_record_batch(array_address, schema_address))
    }
}

impl Drop for HostDecoder<'_> {
    fn drop(&mut self) {
        crate::reader::Java_tech_streamfusion_parquet_NativeParquet_closeParquetDecoder(
            self.vm.get_env().expect("attached benchmark thread"),
            JClass::default(),
            self.handle,
        );
    }
}

fn check_exception(vm: &JavaVM) {
    let env = vm.get_env().expect("attached benchmark thread");
    if env
        .exception_check()
        .expect("check host callback exception")
    {
        env.exception_describe()
            .expect("describe callback exception");
        panic!("production host decoder raised a Java exception");
    }
}
