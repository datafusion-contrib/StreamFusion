use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use jni::objects::{GlobalRef, JObject, JValue};
use jni::{JNIEnv, JavaVM};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;

fn unsupported() -> Error {
    Error::new(ErrorKind::FeatureUnsupported, "Read-only task FileIO")
}

struct Host {
    vm: JavaVM,
    io: GlobalRef,
}

/// Only the live reader owns this bridge; serialized source state contains Java file tasks.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct InputStorage {
    #[serde(skip)]
    host: Option<Arc<Host>>,
}

impl std::fmt::Debug for InputStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StreamFusionInputStorage")
    }
}

impl InputStorage {
    pub(crate) fn file_io(env: &JNIEnv, io: JObject) -> FileIO {
        let storage = Self {
            host: Some(Arc::new(Host {
                vm: env.get_java_vm().expect("Java VM"),
                io: env.new_global_ref(io).expect("Iceberg task input IO"),
            })),
        };
        FileIOBuilder::new(Arc::new(InputFactory(storage))).build()
    }

    fn host(&self) -> Result<&Arc<Host>> {
        self.host.as_ref().ok_or_else(unsupported)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputFactory(InputStorage);

#[typetag::serde(name = "streamfusion-task-input-factory")]
impl StorageFactory for InputFactory {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.0.clone()))
    }
}

fn java_error(env: &mut JNIEnv, failure: jni::errors::Error) -> Error {
    let message = if env.exception_check().unwrap_or(false) {
        let exception = env.exception_occurred().expect("Java IO exception");
        env.exception_clear().expect("Clear Java IO exception");
        env.call_method(exception, "toString", "()Ljava/lang/String;", &[])
            .and_then(|value| value.l())
            .and_then(|value| env.get_string(&value.into()).map(String::from))
            .unwrap_or_else(|_| failure.to_string())
    } else {
        failure.to_string()
    };
    if env.exception_check().unwrap_or(false) {
        let _ = env.exception_clear();
    }
    Error::new(ErrorKind::Unexpected, message)
}

#[async_trait]
#[typetag::serde(name = "streamfusion-task-input")]
impl Storage for InputStorage {
    async fn exists(&self, _: &str) -> Result<bool> {
        Err(unsupported())
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let host = self.host()?;
        let mut env = host
            .vm
            .attach_current_thread()
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let result = env.with_local_frame(4, |env| -> jni::errors::Result<i64> {
            let path = env.new_string(path)?;
            env.call_method(
                host.io.as_obj(),
                "length",
                "(Ljava/lang/String;)J",
                &[JValue::Object(path.as_ref())],
            )?
            .j()
        });
        let size = result.map_err(|e| java_error(&mut env, e))?;
        Ok(FileMetadata {
            size: size.try_into().map_err(|_| unsupported())?,
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let length = self.metadata(path).await?.size;
        self.reader(path).await?.read(0..length).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(HostFile {
            host: self.host()?.clone(),
            path: path.to_owned(),
        }))
    }

    async fn write(&self, _: &str, _: Bytes) -> Result<()> {
        Err(unsupported())
    }
    async fn writer(&self, _: &str) -> Result<Box<dyn FileWrite>> {
        Err(unsupported())
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

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, _: &str) -> Result<OutputFile> {
        Err(unsupported())
    }
}

struct HostFile {
    host: Arc<Host>,
    path: String,
}

#[async_trait]
impl FileRead for HostFile {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let count = range.end.checked_sub(range.start).ok_or_else(unsupported)?;
        let mut bytes = vec![0; usize::try_from(count).map_err(|_| unsupported())?];
        let mut env = self
            .host
            .vm
            .attach_current_thread()
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        for (index, chunk) in bytes.chunks_mut(1 << 20).enumerate() {
            let result = env.with_local_frame(8, |env| -> jni::errors::Result<()> {
                let path = env.new_string(&self.path)?;
                // Java fills this borrowed allocation synchronously and never retains it.
                let target =
                    unsafe { env.new_direct_byte_buffer(chunk.as_mut_ptr(), chunk.len())? };
                env.call_method(
                    self.host.io.as_obj(),
                    "readFully",
                    "(Ljava/lang/String;JLjava/nio/ByteBuffer;)V",
                    &[
                        JValue::Object(path.as_ref()),
                        JValue::Long((range.start + (index as u64 * (1 << 20))) as i64),
                        JValue::Object(target.as_ref()),
                    ],
                )?;
                Ok(())
            });
            result.map_err(|e| java_error(&mut env, e))?;
        }
        Ok(bytes.into())
    }
}
