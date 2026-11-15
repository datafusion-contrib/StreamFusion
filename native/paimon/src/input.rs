use jni::objects::{GlobalRef, JObject, JValue};
use jni::{JNIEnv, JavaVM};
use opendal::raw::{oio, *};
use opendal::{
    Buffer, Capability, EntryMode, Error, ErrorKind, Metadata, OperationContext, Operator, Result,
};
use paimon::io::{FileIO, FileIOBuilder, FileIOProvider};
use std::sync::Arc;

struct Host {
    vm: JavaVM,
    io: GlobalRef,
    max_read_bytes: Option<usize>,
}

impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PaimonJavaFileIO")
    }
}

impl Host {
    fn call<T>(&self, f: impl FnOnce(&mut JNIEnv<'_>) -> jni::errors::Result<T>) -> Result<T> {
        let mut env = self.vm.attach_current_thread().map_err(java_error)?;
        let result = f(&mut env);
        if env.exception_check().map_err(java_error)? {
            let throwable = env.exception_occurred().map_err(java_error)?;
            env.exception_clear().map_err(java_error)?;
            let throwable = env.new_global_ref(throwable).map_err(java_error)?;
            if std::thread::panicking() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Paimon input close failed during unwinding",
                ));
            }
            std::panic::resume_unwind(Box::new(streamfusion_bridge::JavaException(throwable)));
        }
        result.map_err(java_error)
    }

    fn length(&self, path: &str) -> Result<u64> {
        self.call(|env| {
            env.with_local_frame(2, |env| {
                let path = env.new_string(path)?;
                env.call_method(
                    self.io.as_obj(),
                    "length",
                    "(Ljava/lang/String;)J",
                    &[JValue::Object(path.as_ref())],
                )?
                .j()
            })
        })?
        .try_into()
        .map_err(|_| Error::new(ErrorKind::Unexpected, "Negative file length"))
    }
}

fn java_error(error: jni::errors::Error) -> Error {
    Error::new(ErrorKind::Unexpected, error.to_string())
}

#[derive(Debug)]
struct Provider(Arc<Host>);

pub(crate) fn file_io(env: &JNIEnv<'_>, io: JObject<'_>, max_read_bytes: Option<usize>) -> FileIO {
    let host = Arc::new(Host {
        vm: env.get_java_vm().expect("Java VM"),
        io: env.new_global_ref(io).expect("Paimon FileIO"),
        max_read_bytes,
    });
    FileIOBuilder::new("streamfusion")
        .with_provider(Arc::new(Provider(host)))
        .build()
        .expect("Java-backed Paimon FileIO")
}

#[async_trait::async_trait]
impl FileIOProvider for Provider {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        // Bind the original URI to the service. OpenDAL only normalizes the opaque key below.
        let service = Input {
            host: self.0.clone(),
            path: path.to_owned(),
        };
        Ok((
            Operator::from_parts(OperationContext::default(), Arc::new(service)),
            "file".into(),
        ))
    }
}

#[derive(Debug, Clone)]
struct Input {
    host: Arc<Host>,
    path: String,
}

struct Handle {
    host: Arc<Host>,
    id: i64,
    length: u64,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Preserve any in-flight read failure; Java also closes remaining streams on reader close.
        let _ = self.host.call(|env| {
            env.call_method(
                self.host.io.as_obj(),
                "closeFile",
                "(J)V",
                &[JValue::Long(self.id)],
            )?;
            Ok(())
        });
    }
}

impl oio::PositionRead for Input {
    type Handle = Handle;

    async fn open(&self) -> Result<Handle> {
        let length = self.host.length(&self.path)?;
        let id = self.host.call(|env| {
            env.with_local_frame(2, |env| {
                let path = env.new_string(&self.path)?;
                env.call_method(
                    self.host.io.as_obj(),
                    "openFile",
                    "(Ljava/lang/String;)J",
                    &[JValue::Object(path.as_ref())],
                )?
                .j()
            })
        })?;
        Ok(Handle {
            host: self.host.clone(),
            id,
            length,
        })
    }

    async fn read_at(handle: &Handle, offset: u64, size: usize) -> Result<Buffer> {
        let size = (handle.length.saturating_sub(offset)).min(size as u64) as usize;
        let mut bytes = vec![0; size];
        if size != 0 {
            handle.host.call(|env| {
                env.with_local_frame(2, |env| {
                    // Borrowed synchronously; Java never retains this native buffer.
                    let buffer =
                        unsafe { env.new_direct_byte_buffer(bytes.as_mut_ptr(), bytes.len())? };
                    env.call_method(
                        handle.host.io.as_obj(),
                        "readFully",
                        "(JJLjava/nio/ByteBuffer;)V",
                        &[
                            JValue::Long(handle.id),
                            JValue::Long(offset as i64),
                            JValue::Object(buffer.as_ref()),
                        ],
                    )?;
                    Ok(())
                })
            })?;
        }
        Ok(Buffer::from(bytes))
    }
}

impl Service for Input {
    type Reader = oio::PositionReader<Input>;
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("streamfusion", "/", "java-paimon")
    }
    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            ..Default::default()
        }
    }
    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        let mut metadata = Metadata::new(EntryMode::FILE);
        metadata.set_content_length(self.host.length(&self.path)?);
        Ok(RpStat::new(metadata))
    }
    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        let reader = oio::PositionReader::new(self.clone());
        Ok(match self.host.max_read_bytes {
            Some(bytes) => reader.with_max_buf_size(bytes),
            None => reader,
        })
    }
    async fn create_dir(
        &self,
        c: &OperationContext,
        p: &str,
        a: OpCreateDir,
    ) -> Result<RpCreateDir> {
        ().create_dir(c, p, a).await
    }
    fn write(&self, c: &OperationContext, p: &str, a: OpWrite) -> Result<()> {
        ().write(c, p, a)
    }
    fn delete(&self, c: &OperationContext) -> Result<()> {
        ().delete(c)
    }
    fn list(&self, c: &OperationContext, p: &str, a: OpList) -> Result<()> {
        ().list(c, p, a)
    }
    fn copy(&self, c: &OperationContext, f: &str, t: &str, a: OpCopy, o: OpCopier) -> Result<()> {
        ().copy(c, f, t, a, o)
    }
    async fn rename(
        &self,
        c: &OperationContext,
        f: &str,
        t: &str,
        a: OpRename,
    ) -> Result<RpRename> {
        ().rename(c, f, t, a).await
    }
    async fn presign(&self, c: &OperationContext, p: &str, a: OpPresign) -> Result<RpPresign> {
        ().presign(c, p, a).await
    }
}
