use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage};
use iceberg::{Error, ErrorKind, Result};
use jni::objects::{GlobalRef, JByteArray, JObject, JValue};
use jni::{JNIEnv, JavaVM};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

fn unsupported() -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        "Single-file output storage does not own table IO",
    )
}

/// A live JVM stream is task-local and must never enter a checkpoint or serialized table object.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct OutputStorage {
    #[serde(skip)]
    output: Option<Arc<Mutex<Option<JavaOutput>>>>,
}

impl std::fmt::Debug for OutputStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StreamFusionOutputStorage")
    }
}

impl OutputStorage {
    pub(crate) fn new(env: &JNIEnv, output: JObject, chunk: JByteArray) -> Self {
        let output = JavaOutput {
            vm: env.get_java_vm().expect("Java VM"),
            stream: env.new_global_ref(output).expect("Iceberg output stream"),
            chunk: env.new_global_ref(chunk).expect("Iceberg transfer buffer"),
            buffered: Vec::with_capacity(JavaOutput::CHUNK_BYTES),
            closed: false,
        };
        Self {
            output: Some(Arc::new(Mutex::new(Some(output)))),
        }
    }
}

#[async_trait]
#[typetag::serde(name = "streamfusion-task-output")]
impl Storage for OutputStorage {
    async fn exists(&self, _: &str) -> Result<bool> {
        Err(unsupported())
    }
    async fn metadata(&self, _: &str) -> Result<FileMetadata> {
        Err(unsupported())
    }
    async fn read(&self, _: &str) -> Result<Bytes> {
        Err(unsupported())
    }
    async fn reader(&self, _: &str) -> Result<Box<dyn FileRead>> {
        Err(unsupported())
    }
    async fn write(&self, _: &str, _: Bytes) -> Result<()> {
        Err(unsupported())
    }
    async fn writer(&self, _: &str) -> Result<Box<dyn FileWrite>> {
        let output = self
            .output
            .as_ref()
            .ok_or_else(unsupported)?
            .lock()
            .map_err(|_| unsupported())?
            .take()
            .ok_or_else(unsupported)?;
        Ok(Box::new(output))
    }
    async fn delete(&self, _: &str) -> Result<()> {
        Err(unsupported())
    }
    async fn delete_prefix(&self, _: &str) -> Result<()> {
        Err(unsupported())
    }
    async fn delete_stream(&self, _: BoxStream<'static, String>) -> Result<()> {
        Err(unsupported())
    }
    fn new_input(&self, _: &str) -> Result<InputFile> {
        Err(unsupported())
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}

struct JavaOutput {
    vm: JavaVM,
    stream: GlobalRef,
    chunk: GlobalRef,
    buffered: Vec<u8>,
    closed: bool,
}

impl JavaOutput {
    const CHUNK_BYTES: usize = 1 << 20;

    fn emit(&mut self) -> Result<()> {
        if self.buffered.is_empty() {
            return Ok(());
        }
        // Attach the current thread rather than carrying a thread-local JNIEnv into async work.
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let array: &JByteArray = self.chunk.as_obj().into();
        let bytes = unsafe {
            std::slice::from_raw_parts(self.buffered.as_ptr().cast::<i8>(), self.buffered.len())
        };
        env.set_byte_array_region(array, 0, bytes)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        env.call_method(
            self.stream.as_obj(),
            "write",
            "([BII)V",
            &[
                JValue::Object(array.as_ref()),
                JValue::Int(0),
                JValue::Int(bytes.len() as i32),
            ],
        )
        .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        self.buffered.clear();
        Ok(())
    }
}

#[async_trait]
impl FileWrite for JavaOutput {
    async fn write(&mut self, mut bytes: Bytes) -> Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::Unexpected, "Iceberg output closed"));
        }
        while !bytes.is_empty() {
            let count = (Self::CHUNK_BYTES - self.buffered.len()).min(bytes.len());
            self.buffered.extend_from_slice(&bytes.split_to(count));
            if self.buffered.len() == Self::CHUNK_BYTES {
                self.emit()?;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Iceberg output closed twice",
            ));
        }
        self.emit()?;
        // The Java appender owns the actual stream's flush/close and failure cleanup.
        self.closed = true;
        Ok(())
    }
}
