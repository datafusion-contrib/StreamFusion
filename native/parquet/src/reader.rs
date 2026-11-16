//! Parquet decoding over a host-owned filesystem, following Arroyo's batch reader and Comet's
//! C Data ownership boundary. The host retains discovery, credentials and checkpoint state.
use crate::*;
use bytes::Bytes;
use jni::objects::JObject;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use std::io::{Read, Result as IoResult};

struct HostFile {
    vm: jni::JavaVM,
    input: jni::objects::GlobalRef,
    length: u64,
}
impl HostFile {
    fn read(&self, start: u64, out: &mut [u8]) -> IoResult<usize> {
        let size = out
            .len()
            .min(self.length.saturating_sub(start) as usize)
            .min(i32::MAX as usize);
        if size == 0 {
            return Ok(0);
        }
        let mut env = self
            .vm
            .get_env()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        env.with_local_frame(4, |env| -> jni::errors::Result<()> {
            // Java consumes this borrowed buffer synchronously; it never retains the address.
            let buffer = unsafe { env.new_direct_byte_buffer(out.as_mut_ptr(), size)? };
            env.call_method(
                self.input.as_obj(),
                "readFully",
                "(JLjava/nio/ByteBuffer;)V",
                &[
                    jni::objects::JValue::Long(start as i64),
                    jni::objects::JValue::Object(buffer.as_ref()),
                ],
            )?;
            Ok(())
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(size)
    }
}
#[derive(Clone)]
struct HostChunks(Arc<HostFile>);
impl Length for HostChunks {
    fn len(&self) -> u64 {
        self.0.length
    }
}
struct HostRead {
    file: Arc<HostFile>,
    offset: u64,
}
impl Read for HostRead {
    fn read(&mut self, out: &mut [u8]) -> IoResult<usize> {
        let count = self.file.read(self.offset, out)?;
        self.offset += count as u64;
        Ok(count)
    }
}
impl ChunkReader for HostChunks {
    type T = HostRead;
    fn get_read(&self, start: u64) -> ParquetResult<HostRead> {
        Ok(HostRead {
            file: self.0.clone(),
            offset: start,
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if start
            .checked_add(length as u64)
            .is_none_or(|end| end > self.len())
        {
            return Err(ParquetError::EOF("Parquet read exceeds file length".into()));
        }
        let mut bytes = vec![0; length];
        self.get_read(start)?.read_exact(&mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
/// The released Parquet Arrow reader exposes INT96 only through i64 timestamp units. Decode
/// milliseconds and the low 64 nanosecond bits in aligned column batches, then recover the
/// remainder by wrapping subtraction. Both decodings retain columnar nesting and null validity.
fn full_range_readers<T: ChunkReader + Clone + 'static>(
    file: T,
    builder: ParquetRecordBatchReaderBuilder<T>,
    roots: &[usize],
    batch_size: usize,
    groups: &[usize],
) -> (
    ParquetRecordBatchReaderBuilder<T>,
    Option<ParquetRecordBatchReader>,
) {
    let builder = builder.with_row_groups(groups.to_vec());
    let projection = ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
    let has_int96 = builder
        .parquet_schema()
        .columns()
        .iter()
        .enumerate()
        .any(|(i, column)| {
            projection.leaf_included(i) && column.physical_type() == parquet::basic::Type::INT96
        });
    if !has_int96 {
        return (builder, None);
    }
    let mut leaves = builder.parquet_schema().columns().iter();
    let fields: Vec<_> = builder
        .schema()
        .fields()
        .iter()
        .map(|f| int96_millis_field(f, &mut leaves))
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        builder.schema().metadata().clone(),
    ));
    let metadata = ArrowReaderMetadata::try_new(
        builder.metadata().clone(),
        ArrowReaderOptions::new().with_schema(schema),
    )
    .expect("INT96 millisecond schema");
    let fractions = builder
        .with_projection(projection)
        .with_batch_size(batch_size)
        .build()
        .expect("INT96 fractional reader");
    (
        ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
            .with_row_groups(groups.to_vec()),
        Some(fractions),
    )
}

fn int96_millis_field(
    field: &Field,
    leaves: &mut std::slice::Iter<'_, Arc<parquet::schema::types::ColumnDescriptor>>,
) -> Field {
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| Arc::new(int96_millis_field(f, leaves)))
                .collect(),
        ),
        DataType::List(f) => DataType::List(Arc::new(int96_millis_field(f, leaves))),
        DataType::LargeList(f) => DataType::LargeList(Arc::new(int96_millis_field(f, leaves))),
        DataType::FixedSizeList(f, n) => {
            DataType::FixedSizeList(Arc::new(int96_millis_field(f, leaves)), *n)
        }
        DataType::Map(f, sorted) => DataType::Map(Arc::new(int96_millis_field(f, leaves)), *sorted),
        other => {
            let leaf = leaves.next().expect("Parquet Arrow leaf alignment");
            if leaf.physical_type() == parquet::basic::Type::INT96 {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
            } else {
                other.clone()
            }
        }
    };
    field.clone().with_data_type(data_type)
}

fn merge_int96(millis: &ArrayRef, nanos: &ArrayRef) -> ArrayRef {
    use arrow::datatypes::TimeUnit;
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
    if matches!(
        millis.data_type(),
        DataType::Timestamp(TimeUnit::Millisecond, _)
    ) && matches!(
        nanos.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, _)
    ) {
        let ms = millis
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .unwrap();
        let ns = nanos
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        return Arc::new(timestamp_array(ms.iter().zip(ns.iter()).map(|(ms, ns)| {
            ms.zip(ns).map(|(ms, ns)| {
                let fraction = ns.wrapping_sub(ms.wrapping_mul(1_000_000));
                TimestampValue::new(ms, fraction.try_into().expect("INT96 remainder sign"))
                    .expect("INT96 nanosecond remainder")
            })
        })));
    }
    let data = millis.to_data();
    if data.child_data().is_empty() {
        return millis.clone();
    }
    let other = nanos.to_data();
    let children: Vec<_> = data
        .child_data()
        .iter()
        .zip(other.child_data())
        .map(|(a, b)| {
            merge_int96(
                &arrow::array::make_array(a.clone()),
                &arrow::array::make_array(b.clone()),
            )
            .to_data()
        })
        .collect();
    let retype = |f: &Arc<Field>, i: usize| {
        Arc::new(
            f.as_ref()
                .clone()
                .with_data_type(children[i].data_type().clone()),
        )
    };
    let data_type = match data.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| retype(f, i))
                .collect(),
        ),
        DataType::List(f) => DataType::List(retype(f, 0)),
        DataType::LargeList(f) => DataType::LargeList(retype(f, 0)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(retype(f, 0), *n),
        DataType::Map(f, sorted) => DataType::Map(retype(f, 0), *sorted),
        other => other.clone(),
    };
    arrow::array::make_array(
        data.into_builder()
            .data_type(data_type)
            .child_data(children)
            .build()
            .expect("INT96 nested components"),
    )
}

struct ParquetDecoder {
    reader: ParquetRecordBatchReader,
    fractions: Option<ParquetRecordBatchReader>,
    output: SchemaRef,
    names: Vec<String>,
    max_row_group_bytes: i64,
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_createParquetDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    input: JObject<'local>,
    length: jlong,
    schema_address: jlong,
    names: JObjectArray<'local>,
    batch_size: jint,
) -> jlong {
    bridge::jni_guard(env, |env| {
        create_decoder(
            env,
            input,
            length,
            schema_address,
            names,
            batch_size,
            0,
            length,
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_createParquetSplitDecoder<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    input: JObject<'local>,
    length: jlong,
    schema_address: jlong,
    names: JObjectArray<'local>,
    batch_size: jint,
    split_offset: jlong,
    split_length: jlong,
) -> jlong {
    bridge::jni_guard(env, |env| {
        create_decoder(
            env,
            input,
            length,
            schema_address,
            names,
            batch_size,
            split_offset,
            split_length,
        )
    })
}

fn host_file(env: &mut JNIEnv, input: JObject, length: jlong) -> HostChunks {
    assert!(length >= 0, "Negative Parquet file length");
    HostChunks(Arc::new(HostFile {
        vm: env.get_java_vm().expect("JVM"),
        input: env.new_global_ref(input).expect("input stream"),
        length: length as u64,
    }))
}

fn group_start(group: &parquet::file::metadata::RowGroupMetaData) -> u64 {
    group
        .columns()
        .iter()
        .map(|column| column.byte_range().0)
        .min()
        .expect("Parquet row group has no column chunks")
}

fn split_row_groups(
    metadata: &parquet::file::metadata::ParquetMetaData,
    start: i64,
    length: i64,
) -> Vec<usize> {
    assert!(start >= 0 && length >= 0, "Negative Parquet split range");
    let end = start
        .checked_add(length)
        .expect("Parquet split range overflow") as u64;
    metadata
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, group)| group.num_rows() > 0)
        .filter_map(|(index, group)| {
            // Match parquet-mr's half-open midpoint ownership, independent of projection.
            let midpoint = group_start(group)
                .checked_add(
                    u64::try_from(group.compressed_size()).expect("Negative Parquet group size")
                        / 2,
                )
                .expect("Parquet group midpoint overflow");
            (start as u64 <= midpoint && midpoint < end).then_some(index)
        })
        .collect()
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_parquetRowGroupOffsets<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    input: JObject<'local>,
    length: jlong,
) -> jni::sys::jlongArray {
    bridge::jni_guard(env, |env| {
        let builder = ParquetRecordBatchReaderBuilder::try_new(host_file(env, input, length))
            .expect("open Parquet footer");
        let offsets: Vec<i64> = builder
            .metadata()
            .row_groups()
            .iter()
            .filter(|group| group.num_rows() > 0)
            .map(|group| {
                i64::try_from(group_start(group)).expect("Parquet row-group offset overflow")
            })
            .collect();
        assert!(
            offsets
                .iter()
                .all(|offset| *offset >= 0 && *offset < length)
                && offsets.windows(2).all(|pair| pair[0] < pair[1]),
            "Invalid Parquet row-group offsets"
        );
        let result = env
            .new_long_array(offsets.len() as i32)
            .expect("row-group offsets");
        env.set_long_array_region(&result, 0, &offsets)
            .expect("export row-group offsets");
        result.into_raw()
    })
}

#[allow(clippy::too_many_arguments)]
fn create_decoder<'local>(
    env: &mut JNIEnv<'local>,
    input: JObject<'local>,
    length: jlong,
    schema_address: jlong,
    names: JObjectArray<'local>,
    batch_size: jint,
    split_offset: jlong,
    split_length: jlong,
) -> jlong {
    let schema = import_schema(schema_address);
    let names: Vec<String> = read_strings(env, &names)
        .into_iter()
        .map(|n| n.expect("Parquet field name"))
        .collect();
    let file = host_file(env, input, length);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file.clone()).expect("open Parquet file");
    let groups = split_row_groups(builder.metadata(), split_offset, split_length);
    let indices: Vec<usize> = names
        .iter()
        .map(|name| {
            builder
                .schema()
                .index_of(name)
                .unwrap_or_else(|_| panic!("Missing Parquet field {name}"))
        })
        .collect();
    let projection = ProjectionMask::roots(builder.parquet_schema(), indices.clone());
    // Unprojected chunks are never loaded by parquet-rs. Charging them made independent narrow
    // scans compete for the whole file width rather than their actual decoder workspace.
    let max_row_group_bytes = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(index, _)| groups.binary_search(index).is_ok())
        .map(|(_, group)| {
            group
                .columns()
                .iter()
                .enumerate()
                .filter(|(index, _)| projection.leaf_included(*index))
                .fold(0_i64, |bytes, (_, column)| {
                    bytes.saturating_add(
                        column
                            .compressed_size()
                            .saturating_add(column.uncompressed_size()),
                    )
                })
        })
        .max()
        .unwrap_or(0);
    let (builder, fractions) =
        full_range_readers(file, builder, &indices, batch_size as usize, &groups);
    let max_row_group_bytes =
        max_row_group_bytes.saturating_mul(if fractions.is_some() { 2 } else { 1 });
    let reader = builder
        .with_projection(projection)
        .with_batch_size(batch_size as usize)
        .build()
        .expect("Parquet batch reader");
    into_handle(ParquetDecoder {
        reader,
        fractions,
        output: schema,
        names,
        max_row_group_bytes,
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_parquetDecoderMaxRowGroupBytes(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    bridge::jni_guard(env, |_| unsafe {
        (*(handle as *mut ParquetDecoder)).max_row_group_bytes
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_parquetDecoderNext<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jboolean {
    bridge::jni_guard(env, |_env| {
        let decoder = unsafe { &mut *(handle as *mut ParquetDecoder) };
        let Some(batch) = decoder.reader.next() else {
            return 0;
        };
        let batch = batch.expect("decode Parquet batch");
        let fractions = decoder.fractions.as_mut().map(|reader| {
            reader
                .next()
                .expect("INT96 fraction batch")
                .expect("decode INT96 fractions")
        });
        let columns = decoder
            .names
            .iter()
            .zip(decoder.output.fields())
            .map(|(name, field)| {
                let column = batch.column_by_name(name).expect("projected Parquet field");
                let full_range = fractions
                    .as_ref()
                    .and_then(|batch| batch.column_by_name(name))
                    .map(|fraction| merge_int96(column, fraction))
                    .unwrap_or_else(|| column.clone());
                streamfusion_bridge::timestamp::cast_array(&full_range, field.data_type())
                    .expect("Parquet logical type conversion")
            })
            .collect();
        let output =
            RecordBatch::try_new(decoder.output.clone(), columns).expect("Parquet output schema");
        export_record_batch(output, array, schema);
        1
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_parquet_NativeParquet_closeParquetDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    bridge::jni_guard(env, |_env| unsafe {
        drop(from_handle::<ParquetDecoder>(handle));
    })
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;
    use parquet::data_type::{Int96, Int96Type};
    use parquet::file::writer::SerializedFileWriter;
    use streamfusion_bridge::timestamp::{TimestampColumn, TimestampValue};

    #[test]
    fn int96_keeps_wide_dates_fraction_nulls_and_batch_alignment() {
        let expected = [
            Some(TimestampValue::new(-62_135_596_800_000, 123456).unwrap()),
            None,
            Some(TimestampValue::new(-1, 999999).unwrap()),
            Some(TimestampValue::new(9_223_459_200_000, 123456).unwrap()),
            Some(TimestampValue::new(253_402_300_799_999, 999999).unwrap()),
        ];
        for dictionary in [false, true] {
            let schema = Arc::new(
                parquet::schema::parser::parse_message_type("message test { OPTIONAL INT96 ts; }")
                    .unwrap(),
            );
            let props = Arc::new(
                parquet::file::properties::WriterProperties::builder()
                    .set_dictionary_enabled(dictionary)
                    .build(),
            );
            let mut bytes = Vec::new();
            let mut writer = SerializedFileWriter::new(&mut bytes, schema, props).unwrap();
            let values: Vec<_> = expected
                .iter()
                .flatten()
                .map(|value| {
                    let nanos = value.nanos();
                    let day = nanos.div_euclid(86_400_000_000_000) + 2_440_588;
                    let fraction = nanos.rem_euclid(86_400_000_000_000) as u64;
                    let mut raw = Int96::new();
                    raw.set_data(fraction as u32, (fraction >> 32) as u32, day as u32);
                    raw
                })
                .collect();
            for index in 0..2 {
                if index == 1 {
                    let mut empty = writer.next_row_group().unwrap();
                    let mut column = empty.next_column().unwrap().unwrap();
                    column
                        .typed::<Int96Type>()
                        .write_batch(&[], None, None)
                        .unwrap();
                    column.close().unwrap();
                    empty.close().unwrap();
                }
                let mut group_values = values.clone();
                if index == 1 {
                    group_values.reverse();
                }
                let mut group = writer.next_row_group().unwrap();
                let mut column = group.next_column().unwrap().unwrap();
                column
                    .typed::<Int96Type>()
                    .write_batch(&group_values, Some(&[1, 0, 1, 1, 1]), None)
                    .unwrap();
                column.close().unwrap();
                group.close().unwrap();
            }
            writer.close().unwrap();
            let bytes = Bytes::from(bytes);
            for groups in [vec![0], vec![2], vec![0, 2], vec![]] {
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
                let midpoint = group_start(builder.metadata().row_group(2))
                    + builder.metadata().row_group(2).compressed_size() as u64 / 2;
                assert_eq!(
                    split_row_groups(builder.metadata(), 0, midpoint as i64),
                    vec![0]
                );
                assert_eq!(
                    split_row_groups(
                        builder.metadata(),
                        midpoint as i64,
                        bytes.len() as i64 - midpoint as i64
                    ),
                    vec![2]
                );
                let (builder, fractions) =
                    full_range_readers(bytes.clone(), builder, &[0], 2, &groups);
                let mut fractions = fractions.unwrap();
                let reader = builder.with_batch_size(2).build().unwrap();
                let mut actual = Vec::new();
                for batch in reader {
                    let batch = batch.unwrap();
                    let fraction = fractions.next().unwrap().unwrap();
                    let pair = merge_int96(batch.column(0), fraction.column(0));
                    let column = TimestampColumn::try_new(pair.as_ref()).unwrap();
                    actual.extend((0..pair.len()).map(|row| {
                        if column.is_null(row) {
                            None
                        } else {
                            Some(column.value(row).unwrap())
                        }
                    }));
                }
                assert!(fractions.next().is_none());
                let second = [expected[4], None, expected[3], expected[2], expected[0]];
                let selected: Vec<_> = groups
                    .iter()
                    .flat_map(|index| if *index == 0 { expected } else { second })
                    .collect();
                assert_eq!(actual, selected);
            }
        }
    }
}

/// In-memory fixture adapter; retains the production projection and INT96 reader selection.
pub(crate) fn decode_for_benchmark(bytes: &[u8], batch_rows: usize) -> Vec<RecordBatch> {
    let file = bytes::Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
    let roots: Vec<_> = (0..builder.schema().fields().len()).collect();
    let groups: Vec<_> = (0..builder.metadata().num_row_groups()).collect();
    let (builder, mut fractions) = full_range_readers(file, builder, &roots, batch_rows, &groups);
    builder
        .with_batch_size(batch_rows)
        .build()
        .unwrap()
        .map(|batch| {
            let batch = batch.unwrap();
            let Some(reader) = fractions.as_mut() else {
                return batch;
            };
            let fractional = reader.next().unwrap().unwrap();
            assert_eq!(batch.num_rows(), fractional.num_rows());
            let columns: Vec<_> = batch
                .columns()
                .iter()
                .zip(fractional.columns())
                .map(|(millis, nanos)| merge_int96(millis, nanos))
                .collect();
            let fields: Vec<_> = batch
                .schema()
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| {
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(column.data_type().clone())
                })
                .collect();
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    fields,
                    batch.schema().metadata().clone(),
                )),
                columns,
            )
            .unwrap()
        })
        .collect()
}
