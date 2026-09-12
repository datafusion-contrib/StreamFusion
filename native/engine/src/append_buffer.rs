use crate::*;
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use datafusion::common::Result;
use datafusion::execution::disk_manager::{DiskManager, DiskManagerMode, RefCountedTempFile};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

struct Spill {
    file: RefCountedTempFile,
    bytes: u64,
    remaining_rows: usize,
}

#[derive(Default)]
struct Bucket {
    batches: VecDeque<RecordBatch>,
    reader: Option<StreamReader<Box<dyn Read>>>,
    spills: VecDeque<Spill>,
    memory_bytes: usize,
    disk_bytes: u64,
}

/// Local, disposable buffering. Checkpoints drain these batches into Paimon's committed files;
/// recovery replays the source, never a local spill. Like Comet, DataFusion owns temporary files.
struct AppendBuffer {
    buckets: HashMap<i32, Bucket>,
    disk: Arc<DiskManager>,
    codec: i32,
    zstd_level: i32,
    max_disk_bytes: u64,
}

impl AppendBuffer {
    fn new(
        directories: Vec<String>,
        codec: i32,
        zstd_level: i32,
        max_disk_bytes: u64,
    ) -> Result<Self> {
        assert!((0..=2).contains(&codec), "unsupported Arrow spill codec");
        Ok(Self {
            buckets: HashMap::default(),
            disk: Arc::new(
                DiskManager::builder()
                    .with_max_temp_directory_size(u64::MAX)
                    .with_mode(DiskManagerMode::Directories(
                        directories.into_iter().map(Into::into).collect(),
                    ))
                    .build()?,
            ),
            codec,
            zstd_level,
            max_disk_bytes,
        })
    }

    fn push(&mut self, id: i32, batch: RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }
        let bucket = self.buckets.entry(id).or_default();
        assert!(
            bucket.reader.is_none(),
            "cannot append while draining a spill"
        );
        bucket.memory_bytes += batch.get_array_memory_size();
        bucket.batches.push_back(batch);
    }

    fn bytes(&self) -> usize {
        self.buckets
            .values()
            .map(|bucket| bucket.memory_bytes)
            .sum()
    }

    fn spill_largest(&mut self) -> Result<i32> {
        let Some((&id, bucket)) = self.buckets.iter_mut().max_by_key(|(_, b)| b.memory_bytes)
        else {
            return Ok(-1);
        };
        if bucket.memory_bytes == 0 {
            return Ok(-1);
        }
        // Paimon's disk allowance is a soft per-writer limit checked before the next spill.
        if bucket.disk_bytes >= self.max_disk_bytes {
            return Ok(id);
        }
        let file = self.disk.create_tmp_file("Paimon append buffer")?;
        let output = BufWriter::new(File::create(file.path())?);
        match self.codec {
            0 => {
                write_batches(output, &bucket.batches)?.flush()?;
            }
            1 => {
                let writer = zstd::stream::write::Encoder::new(output, self.zstd_level)?;
                write_batches(writer, &bucket.batches)?.finish()?.flush()?;
            }
            2 => {
                let writer = lz4_flex::frame::FrameEncoder::new(output);
                write_batches(writer, &bucket.batches)?
                    .finish()
                    .map_err(std::io::Error::other)?
                    .flush()?;
            }
            _ => unreachable!(),
        }
        let bytes = std::fs::metadata(file.path())?.len();
        bucket.spills.push_back(Spill {
            file,
            bytes,
            remaining_rows: bucket.batches.iter().map(RecordBatch::num_rows).sum(),
        });
        bucket.disk_bytes += bytes;
        bucket.batches.clear();
        bucket.memory_bytes = 0;
        Ok(-1)
    }

    fn next(&mut self, id: i32) -> Result<Option<RecordBatch>> {
        let Some(bucket) = self.buckets.get_mut(&id) else {
            return Ok(None);
        };
        while let Some(spill) = bucket.spills.front_mut() {
            if bucket.reader.is_none() {
                let input = BufReader::new(File::open(spill.file.path())?);
                let input: Box<dyn Read> = match self.codec {
                    0 => Box::new(input),
                    1 => Box::new(zstd::stream::read::Decoder::new(input)?),
                    2 => Box::new(lz4_flex::frame::FrameDecoder::new(input)),
                    _ => unreachable!(),
                };
                bucket.reader = Some(StreamReader::try_new(input, None)?);
            }
            if let Some(batch) = bucket.reader.as_mut().unwrap().next() {
                let batch = batch?;
                spill.remaining_rows = spill
                    .remaining_rows
                    .checked_sub(batch.num_rows())
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(
                            "Arrow spill has excess rows".into(),
                        )
                    })?;
                return Ok(Some(batch));
            }
            if spill.remaining_rows != 0 {
                return Err(datafusion::common::DataFusionError::Execution(
                    "Arrow spill ended before all rows were read".into(),
                ));
            }
            bucket.reader = None;
            bucket.disk_bytes -= bucket.spills.pop_front().unwrap().bytes;
        }
        if let Some(batch) = bucket.batches.pop_front() {
            bucket.memory_bytes -= batch.get_array_memory_size();
            return Ok(Some(batch));
        }
        self.buckets.remove(&id);
        Ok(None)
    }
}

fn write_batches<W: Write>(output: W, batches: &VecDeque<RecordBatch>) -> Result<W> {
    let mut writer = StreamWriter::try_new(output, &batches.front().unwrap().schema())?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(writer.into_inner()?)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createAppendBuffer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    directories: JObjectArray<'local>,
    codec: jint,
    zstd_level: jint,
    max_disk_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        into_handle(
            AppendBuffer::new(
                read_string_array(env, &directories),
                codec,
                zstd_level,
                max_disk_bytes as u64,
            )
            .expect("create Arrow spill buffer"),
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_appendBufferPush<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    bucket: jint,
    array_address: jlong,
    schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &mut *(handle as *mut AppendBuffer) }
            .push(bucket, import_record_batch(array_address, schema_address));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_appendBufferBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const AppendBuffer) }.bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_appendBufferSpillLargest<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &mut *(handle as *mut AppendBuffer) }
            .spill_largest()
            .expect("spill Arrow batches")
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_appendBufferNext<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    bucket: jint,
    array_address: jlong,
    schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        match unsafe { &mut *(handle as *mut AppendBuffer) }
            .next(bucket)
            .expect("read Arrow spill")
        {
            Some(batch) => {
                export_record_batch(batch, array_address, schema_address);
                1
            }
            None => 0,
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeAppendBuffer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<AppendBuffer>(handle));
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffer(codec: i32, disk_limit: u64) -> AppendBuffer {
        AppendBuffer::new(
            vec![std::env::temp_dir().to_string_lossy().into_owned()],
            codec,
            3,
            disk_limit,
        )
        .unwrap()
    }

    fn batch(start: i64, count: usize) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from_iter_values(start..start + count as i64)) as ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn drains_multiple_spills_before_memory_in_arrival_order_for_every_codec() {
        for codec in 0..=2 {
            let mut buffer = buffer(codec, u64::MAX);
            buffer.push(0, batch(0, 3));
            buffer.push(1, batch(100, 1));
            assert_eq!(buffer.spill_largest().unwrap(), -1);
            assert_eq!(buffer.buckets[&0].memory_bytes, 0);
            assert!(buffer.buckets[&1].memory_bytes > 0);
            let first_path = buffer.buckets[&0].spills[0].file.path().to_owned();
            buffer.push(0, batch(3, 4));
            buffer.spill_largest().unwrap();
            buffer.push(0, batch(7, 2));
            let mut ids = Vec::new();
            while let Some(batch) = buffer.next(0).unwrap() {
                ids.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
            }
            assert_eq!(ids, (0..9).collect::<Vec<_>>());
            assert!(!first_path.exists());
            assert_eq!(buffer.next(1).unwrap().unwrap(), batch(100, 1));
            assert!(buffer.next(1).unwrap().is_none());
            assert_eq!(buffer.bytes(), 0);
            assert!(buffer.buckets.is_empty());
            buffer.push(0, batch(200, 2));
            assert_eq!(buffer.next(0).unwrap().unwrap(), batch(200, 2));
        }
    }

    #[test]
    fn full_disk_requests_a_flush_without_losing_pending_rows() {
        let mut buffer = buffer(1, 1);
        buffer.push(2, batch(0, 10));
        assert_eq!(buffer.spill_largest().unwrap(), -1);
        buffer.push(2, batch(10, 10));
        assert_eq!(buffer.spill_largest().unwrap(), 2);
        assert_eq!(buffer.next(2).unwrap().unwrap(), batch(0, 10));
        assert_eq!(buffer.next(2).unwrap().unwrap(), batch(10, 10));
        assert!(buffer.next(2).unwrap().is_none());
    }

    #[test]
    fn close_deletes_spills_even_during_a_partial_read() {
        let mut buffer = buffer(2, u64::MAX);
        buffer.push(0, batch(0, 10));
        buffer.spill_largest().unwrap();
        let path = buffer.buckets[&0].spills[0].file.path().to_owned();
        buffer.next(0).unwrap();
        drop(buffer);
        assert!(!path.exists());
    }

    #[test]
    fn failed_spill_retains_memory_and_truncated_input_fails() {
        let mut buffer = buffer(0, u64::MAX);
        buffer.push(0, batch(0, 10));
        let disk = Arc::clone(&buffer.disk);
        buffer.disk = Arc::new(
            DiskManager::builder()
                .with_mode(DiskManagerMode::Disabled)
                .build()
                .unwrap(),
        );
        assert!(buffer.spill_largest().is_err());
        assert!(buffer.bytes() > 0);
        buffer.disk = disk;
        buffer.spill_largest().unwrap();
        let path = buffer.buckets[&0].spills[0].file.path().to_owned();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(9)
            .unwrap();
        assert!(buffer.next(0).is_err());
        drop(buffer);
        assert!(!path.exists());
    }

    #[test]
    fn missing_complete_batches_cannot_look_like_a_successful_eof() {
        let mut buffer = buffer(0, u64::MAX);
        let first = batch(0, 10);
        let prefix = write_batches(Vec::new(), &VecDeque::from([first.clone()])).unwrap();
        buffer.push(0, first);
        buffer.push(0, batch(10, 10));
        buffer.spill_largest().unwrap();
        let path = buffer.buckets[&0].spills[0].file.path();
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_len((prefix.len() - 8) as u64)
            .unwrap();
        assert_eq!(buffer.next(0).unwrap().unwrap(), batch(0, 10));
        assert!(buffer.next(0).is_err());
    }
}
