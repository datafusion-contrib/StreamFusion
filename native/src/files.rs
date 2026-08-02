use crate::*;

/// Writes one batch to a Parquet file at `path` with default settings — a test fixture helper for
/// the source-side tests (the sink writes through `ParquetEncoder`, never to a path).
#[cfg(test)]
pub(crate) fn write_parquet(batch: &RecordBatch, path: &str) {
    let file = std::fs::File::create(path).expect("failed to create parquet file");
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
        .expect("failed to create parquet writer");
    writer.write(batch).expect("failed to write batch");
    writer.close().expect("failed to close parquet writer");
}

/// In-memory landing zone for encoded Parquet bytes. The `ArrowWriter` owns one clone as its sink
/// and the encoder keeps another to drain, so encoded row groups become visible to the JVM without
/// closing the writer (Arroyo's SharedBuffer shape). A read offset defers reclaiming consumed bytes
/// until the buffer fully drains, so repeated partial drains never memmove the tail.
#[derive(Clone, Default)]
pub(crate) struct SharedBuffer {
    inner: Arc<Mutex<DrainableBuffer>>,
}

#[derive(Default)]
struct DrainableBuffer {
    bytes: Vec<u8>,
    read: usize,
}

impl std::io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.lock().unwrap().bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SharedBuffer {
    /// Copies up to `out.len()` buffered bytes into `out` and returns the count; 0 means drained.
    pub(crate) fn drain_into(&self, out: &mut [u8]) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let n = (inner.bytes.len() - inner.read).min(out.len());
        let start = inner.read;
        out[..n].copy_from_slice(&inner.bytes[start..start + n]);
        inner.read += n;
        if inner.read == inner.bytes.len() {
            inner.bytes.clear();
            inner.read = 0;
        }
        n
    }
}

/// The writer settings the JVM's config translator resolved from the table DDL and cluster config.
/// Every value here is already effective — defaults applied, overrides resolved — so an unknown key
/// is a translator bug, not user input, and panics.
struct EncoderConfig {
    compression: parquet::basic::Compression,
    block_size: usize,
    page_size: usize,
    dictionary_page_size: usize,
    enable_dictionary: bool,
    writer_version: parquet::file::properties::WriterVersion,
    timestamp_unit: arrow::datatypes::TimeUnit,
}

impl EncoderConfig {
    fn parse(keys: &[String], values: &[String]) -> EncoderConfig {
        use parquet::basic::{Compression, GzipLevel, ZstdLevel};
        use parquet::file::properties::WriterVersion;

        let mut compression = "SNAPPY".to_string();
        // Flink-effective codec levels: parquet-mr's ZstandardCodec defaults to 3 (parquet-rs to 1)
        // and gzip inherits zlib's default 6.
        let mut zstd_level = 3i32;
        let mut gzip_level = 6u32;
        let mut config = EncoderConfig {
            compression: Compression::SNAPPY,
            block_size: 128 * 1024 * 1024,
            page_size: 1024 * 1024,
            dictionary_page_size: 1024 * 1024,
            enable_dictionary: true,
            writer_version: WriterVersion::PARQUET_1_0,
            timestamp_unit: arrow::datatypes::TimeUnit::Microsecond,
        };
        for (key, value) in keys.iter().zip(values) {
            match key.as_str() {
                "compression" => compression = value.clone(),
                "compression.zstd.level" => zstd_level = value.parse().expect("invalid zstd level"),
                "compression.gzip.level" => gzip_level = value.parse().expect("invalid gzip level"),
                "block.size" => config.block_size = value.parse().expect("invalid block size"),
                "page.size" => config.page_size = value.parse().expect("invalid page size"),
                "dictionary.page.size" => {
                    config.dictionary_page_size =
                        value.parse().expect("invalid dictionary page size")
                }
                "enable.dictionary" => {
                    config.enable_dictionary = value.parse().expect("invalid dictionary flag")
                }
                "writer.version" => {
                    config.writer_version = match value.as_str() {
                        "1" => WriterVersion::PARQUET_1_0,
                        "2" => WriterVersion::PARQUET_2_0,
                        other => panic!("unsupported parquet writer version {other}"),
                    }
                }
                "timestamp.unit" => {
                    config.timestamp_unit = match value.as_str() {
                        "millis" => arrow::datatypes::TimeUnit::Millisecond,
                        "micros" => arrow::datatypes::TimeUnit::Microsecond,
                        "nanos" => arrow::datatypes::TimeUnit::Nanosecond,
                        other => panic!("unsupported parquet timestamp unit {other}"),
                    }
                }
                other => panic!("unknown parquet encoder option {other}"),
            }
        }
        config.compression = match compression.as_str() {
            "UNCOMPRESSED" => Compression::UNCOMPRESSED,
            "SNAPPY" => Compression::SNAPPY,
            "GZIP" => {
                Compression::GZIP(GzipLevel::try_new(gzip_level).expect("invalid gzip level"))
            }
            "ZSTD" => {
                Compression::ZSTD(ZstdLevel::try_new(zstd_level).expect("invalid zstd level"))
            }
            other => panic!("unsupported parquet compression {other}"),
        };
        config
    }

    fn writer_properties(&self) -> parquet::file::properties::WriterProperties {
        parquet::file::properties::WriterProperties::builder()
            .set_compression(self.compression)
            .set_writer_version(self.writer_version)
            .set_data_page_size_limit(self.page_size)
            .set_dictionary_page_size_limit(self.dictionary_page_size)
            .set_dictionary_enabled(self.enable_dictionary)
            // parquet-mr row groups are bounded by bytes only (parquet.block.size); parquet-rs
            // defaults to a 1M-row cap with no byte bound, so swap the two.
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(Some(self.block_size))
            // parquet-mr does not truncate chunk-level min/max statistics; parquet-rs defaults to 64.
            .set_statistics_truncate_length(None)
            .build()
    }
}

/// The Flink type a written column takes on: timestamps land in the configured INT64 unit
/// (`timestamp.time.unit`, always timezone-less) and TIME narrows to millisecond INT32, matching the
/// host writer. Everything else is written as it arrives from the canonical Arrow encoding.
fn write_data_type(source: &DataType, timestamp_unit: arrow::datatypes::TimeUnit) -> DataType {
    use arrow::datatypes::TimeUnit;
    match source {
        DataType::Timestamp(_, None) => DataType::Timestamp(timestamp_unit, None),
        DataType::Time32(_) | DataType::Time64(_) => DataType::Time32(TimeUnit::Millisecond),
        other => other.clone(),
    }
}

/// Converts one column to its write type. Unit narrowing floors the value (Flink's TimestampData
/// keeps a non-negative sub-millisecond part, so its arithmetic floors too); a plain `/` would
/// round pre-1970 values toward zero and diverge from the host by one unit.
fn convert_column(column: &ArrayRef, target: &DataType) -> ArrayRef {
    use arrow::array::{Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray};
    use arrow::compute::kernels::arity::unary;
    use arrow::datatypes::{
        Time32MillisecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    };

    if column.data_type() == target {
        return column.clone();
    }
    match (column.data_type(), target) {
        (DataType::Timestamp(TimeUnit::Nanosecond, None), DataType::Timestamp(unit, None)) => {
            let nanos = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp column was not nanosecond");
            match unit {
                TimeUnit::Microsecond => {
                    Arc::new(unary::<_, _, TimestampMicrosecondType>(nanos, |v| {
                        v.div_euclid(1_000)
                    }))
                }
                TimeUnit::Millisecond => {
                    Arc::new(unary::<_, _, TimestampMillisecondType>(nanos, |v| {
                        v.div_euclid(1_000_000)
                    }))
                }
                other => panic!("unsupported timestamp write unit {other:?}"),
            }
        }
        (DataType::Time32(TimeUnit::Second), DataType::Time32(TimeUnit::Millisecond)) => {
            let seconds = column
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .expect("time column was not second");
            Arc::new(unary::<_, _, Time32MillisecondType>(seconds, |v| v * 1_000))
        }
        (DataType::Time64(TimeUnit::Microsecond), DataType::Time32(TimeUnit::Millisecond)) => {
            let micros = column
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("time column was not microsecond");
            Arc::new(unary::<_, _, Time32MillisecondType>(micros, |v| {
                v.div_euclid(1_000) as i32
            }))
        }
        (DataType::Time64(TimeUnit::Nanosecond), DataType::Time32(TimeUnit::Millisecond)) => {
            let nanos = column
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("time column was not nanosecond");
            Arc::new(unary::<_, _, Time32MillisecondType>(nanos, |v| {
                v.div_euclid(1_000_000) as i32
            }))
        }
        (source, target) => panic!("no write conversion from {source:?} to {target:?}"),
    }
}

/// The smallest byte width whose signed range covers `precision` decimal digits — Flink's
/// `computeMinBytesForDecimalPrecision`, mirrored with the same floating-point comparison so the
/// FIXED_LEN_BYTE_ARRAY lengths agree digit for digit.
fn decimal_min_bytes(precision: i32) -> i32 {
    let mut num_bytes = 1;
    while 2f64.powi(8 * num_bytes - 1) < 10f64.powi(precision) {
        num_bytes += 1;
    }
    num_bytes
}

/// The Parquet leaf Flink's schema converter produces for a write-typed field. This is forced onto
/// the `ArrowWriter` as an explicit descriptor because the arrow-rs converter disagrees with Flink
/// on decimals (INT32/INT64 for small precision where Flink always writes FIXED_LEN_BYTE_ARRAY)
/// and on TIME's UTC-adjustment flag.
fn flink_parquet_leaf(field: &Field) -> parquet::schema::types::Type {
    use parquet::basic::{
        LogicalType, Repetition, TimeUnit as ParquetTimeUnit, Type as PhysicalType,
    };
    use parquet::schema::types::Type as ParquetType;

    let builder = match field.data_type() {
        DataType::Boolean => {
            ParquetType::primitive_type_builder(field.name(), PhysicalType::BOOLEAN)
        }
        DataType::Int8 => ParquetType::primitive_type_builder(field.name(), PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            })),
        DataType::Int16 => ParquetType::primitive_type_builder(field.name(), PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            })),
        DataType::Int32 => ParquetType::primitive_type_builder(field.name(), PhysicalType::INT32),
        DataType::Int64 => ParquetType::primitive_type_builder(field.name(), PhysicalType::INT64),
        DataType::Float32 => ParquetType::primitive_type_builder(field.name(), PhysicalType::FLOAT),
        DataType::Float64 => {
            ParquetType::primitive_type_builder(field.name(), PhysicalType::DOUBLE)
        }
        DataType::Utf8 => {
            ParquetType::primitive_type_builder(field.name(), PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
        }
        DataType::Binary => {
            ParquetType::primitive_type_builder(field.name(), PhysicalType::BYTE_ARRAY)
        }
        DataType::Date32 => ParquetType::primitive_type_builder(field.name(), PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Date)),
        // Flink writes TIME through the legacy TIME_MILLIS converted type, whose logical
        // equivalent carries isAdjustedToUTC=true.
        DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
            ParquetType::primitive_type_builder(field.name(), PhysicalType::INT32)
                .with_logical_type(Some(LogicalType::Time {
                    is_adjusted_to_u_t_c: true,
                    unit: ParquetTimeUnit::MILLIS,
                }))
        }
        DataType::Timestamp(unit, None) => {
            ParquetType::primitive_type_builder(field.name(), PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: false,
                    unit: match unit {
                        arrow::datatypes::TimeUnit::Millisecond => ParquetTimeUnit::MILLIS,
                        arrow::datatypes::TimeUnit::Microsecond => ParquetTimeUnit::MICROS,
                        arrow::datatypes::TimeUnit::Nanosecond => ParquetTimeUnit::NANOS,
                        other => panic!("unsupported timestamp write unit {other:?}"),
                    },
                }))
        }
        DataType::Decimal128(precision, scale) => {
            ParquetType::primitive_type_builder(field.name(), PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::Decimal {
                    precision: *precision as i32,
                    scale: *scale as i32,
                }))
                .with_precision(*precision as i32)
                .with_scale(*scale as i32)
                .with_length(decimal_min_bytes(*precision as i32))
        }
        other => panic!("type {other:?} has no Flink parquet mapping"),
    };
    let repetition = if field.is_nullable() {
        Repetition::OPTIONAL
    } else {
        Repetition::REQUIRED
    };
    builder
        .with_repetition(repetition)
        .build()
        .expect("failed to build parquet leaf")
}

/// Encodes Arrow batches into Parquet bytes in memory; the JVM drains the bytes into whatever
/// Flink output stream the part file lives behind. Owning only the encoding — never the IO or the
/// file lifecycle — is what lets the host reuse Flink's filesystems, rolling, and exactly-once
/// commit unchanged.
pub(crate) struct ParquetEncoder {
    writer: Option<parquet::arrow::ArrowWriter<SharedBuffer>>,
    buffer: SharedBuffer,
    write_schema: SchemaRef,
    /// Indices of the written (non-partition) columns in the incoming full-row batches; partition
    /// values live in the directory path, so their columns are projected out (zero-copy).
    projection: Vec<usize>,
}

impl ParquetEncoder {
    pub(crate) fn new(
        full_schema: SchemaRef,
        partition_columns: &[usize],
        config_keys: &[String],
        config_values: &[String],
    ) -> ParquetEncoder {
        let config = EncoderConfig::parse(config_keys, config_values);
        let projection: Vec<usize> = (0..full_schema.fields().len())
            .filter(|index| !partition_columns.contains(index))
            .collect();
        let write_fields: Vec<Field> = projection
            .iter()
            .map(|&index| {
                let field = full_schema.field(index);
                Field::new(
                    field.name(),
                    write_data_type(field.data_type(), config.timestamp_unit),
                    field.is_nullable(),
                )
            })
            .collect();
        let write_schema = Arc::new(Schema::new(write_fields));

        let root = parquet::schema::types::Type::group_type_builder("flink_schema")
            .with_fields(
                write_schema
                    .fields()
                    .iter()
                    .map(|field| Arc::new(flink_parquet_leaf(field)))
                    .collect(),
            )
            .build()
            .expect("failed to build parquet schema");
        let descriptor = parquet::schema::types::SchemaDescriptor::new(Arc::new(root));

        let buffer = SharedBuffer::default();
        let options = parquet::arrow::arrow_writer::ArrowWriterOptions::new()
            .with_properties(config.writer_properties())
            .with_parquet_schema(descriptor)
            // Flink files carry no embedded Arrow schema, and ours must not either: the forced
            // descriptor diverges from what the arrow-rs converter would emit, so an embedded
            // schema would contradict the physical layout for readers that trust it.
            .with_skip_arrow_metadata(true);
        let writer = parquet::arrow::ArrowWriter::try_new_with_options(
            buffer.clone(),
            write_schema.clone(),
            options,
        )
        .expect("failed to create parquet encoder");
        ParquetEncoder {
            writer: Some(writer),
            buffer,
            write_schema,
            projection,
        }
    }

    pub(crate) fn write(&mut self, batch: &RecordBatch) {
        let columns: Vec<ArrayRef> = self
            .projection
            .iter()
            .zip(self.write_schema.fields())
            .map(|(&index, field)| convert_column(batch.column(index), field.data_type()))
            .collect();
        let batch = RecordBatch::try_new(self.write_schema.clone(), columns)
            .expect("write batch did not match the write schema");
        self.writer
            .as_mut()
            .expect("parquet encoder already finished")
            .write(&batch)
            .expect("failed to encode batch");
    }

    /// Writes the footer into the buffer; the handle stays alive so the JVM can drain it.
    pub(crate) fn finish(&mut self) {
        self.writer
            .take()
            .expect("parquet encoder already finished")
            .close()
            .expect("failed to finish parquet file");
    }

    pub(crate) fn drain_into(&self, out: &mut [u8]) -> usize {
        self.buffer.drain_into(out)
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_nativeBuildVersion<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jstring {
    crate::bridge::Java_io_github_jordepic_streamfusion_Native_version(env, class)
}

/// Creates a Parquet encoder for the sink: `schemaAddress` carries the full row schema through the
/// C Data Interface, `partitionColumns` the indices written to the path instead of the file, and
/// the key/value arrays the resolved writer settings. Returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_createParquetEncoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
    partition_columns: JIntArray<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let schema = import_schema(schema_address);
        let partition_columns = read_columns(&env, &partition_columns);
        let keys = required_strings(&mut env, &config_keys);
        let values = required_strings(&mut env, &config_values);
        into_handle(ParquetEncoder::new(
            schema,
            &partition_columns,
            &keys,
            &values,
        ))
    })
}

fn required_strings(env: &mut JNIEnv, values: &JObjectArray) -> Vec<String> {
    read_strings(env, values)
        .into_iter()
        .map(|value| value.expect("config entry was null"))
        .collect()
}

/// Encodes an Arrow batch the JVM exported into the open Parquet stream behind `handle`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_parquetEncoderWrite<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let encoder = unsafe { &mut *(handle as *mut ParquetEncoder) };
        let batch = import_record_batch(in_array_address, in_schema_address);
        encoder.write(&batch);
    })
}

/// Copies buffered encoded bytes into `chunk`, returning the count (0 = drained). The chunk is
/// pinned critically for the duration of one memcpy — the only copy between the encoder's buffer
/// and the Flink output stream the JVM writes it to.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_parquetEncoderDrain<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    chunk: JByteArray<'local>,
) -> jint {
    crate::bridge::jni_guard(env, move |env| {
        use jni::objects::ReleaseMode;

        let encoder = unsafe { &*(handle as *const ParquetEncoder) };
        let mut elements = unsafe { env.get_array_elements_critical(&chunk, ReleaseMode::CopyBack) }
            .expect("failed to pin drain chunk");
        let out =
            unsafe { std::slice::from_raw_parts_mut(elements.as_mut_ptr() as *mut u8, elements.len()) };
        encoder.drain_into(out) as jint
    })
}

/// Writes the Parquet footer into the buffer. The handle stays open so the JVM can drain the
/// remaining bytes; release it with `closeParquetEncoder`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_parquetEncoderFinish<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let encoder = unsafe { &mut *(handle as *mut ParquetEncoder) };
        encoder.finish();
    })
}

/// Releases a Parquet encoder handle; also the abort path for a part file that never finished.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_closeParquetEncoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<ParquetEncoder>(handle));
        }
    })
}

/// Splits one batch into per-partition sub-batches — Arroyo's Partitioner::partition shape: the
/// partition-key columns row-convert to comparable keys, rows group by key in first-seen order, and
/// one permutation take() reorders every column so each group is a zero-copy slice. Groups keep the
/// full row schema; the JVM reads the partition values off row 0 to route the group to its bucket
/// and the encoder projects the columns out of the written file.
pub(crate) fn split_by_partition_columns(
    batch: &RecordBatch,
    partition_columns: &[usize],
) -> Vec<RecordBatch> {
    let key_arrays: Vec<ArrayRef> =
        partition_columns.iter().map(|&index| batch.column(index).clone()).collect();
    let converter = RowConverter::new(
        key_arrays.iter().map(|array| SortField::new(array.data_type().clone())).collect(),
    )
    .expect("failed to build partition key converter");
    let rows = converter.convert_columns(&key_arrays).expect("failed to convert partition keys");

    let mut groups: HashMap<Row, Vec<u32>> = HashMap::default();
    let mut order: Vec<Row> = Vec::new();
    for index in 0..batch.num_rows() {
        let key = rows.row(index);
        groups
            .entry(key)
            .or_insert_with(|| {
                order.push(key);
                Vec::new()
            })
            .push(index as u32);
    }
    // Streaming batches are frequently single-partition (time partitions, pre-shuffled keys), so
    // skip the permutation when there is nothing to reorder.
    if order.len() == 1 {
        return vec![batch.clone()];
    }

    let mut permutation: Vec<u32> = Vec::with_capacity(batch.num_rows());
    let mut lengths: Vec<usize> = Vec::with_capacity(order.len());
    for key in &order {
        let indices = &groups[key];
        permutation.extend_from_slice(indices);
        lengths.push(indices.len());
    }
    let permutation = UInt32Array::from(permutation);
    let permuted_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &permutation, None).expect("failed to permute batch"))
        .collect();
    let permuted =
        RecordBatch::try_new(batch.schema(), permuted_columns).expect("failed to rebuild batch");

    let mut slices = Vec::with_capacity(lengths.len());
    let mut offset = 0;
    for length in lengths {
        slices.push(permuted.slice(offset, length));
        offset += length;
    }
    slices
}

/// Splits an Arrow batch the JVM exported by its partition-key columns and returns a handle to the
/// resulting groups, pulled one at a time with `nextPartitionSlice` and released with
/// `closePartitionSplit`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_splitByPartitionColumns<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    partition_columns: JIntArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let partition_columns = read_columns(&env, &partition_columns);
        let slices = split_by_partition_columns(&batch, &partition_columns);
        into_handle(PartitionSplit { slices: slices.into_iter().rev().collect() })
    })
}

/// Per-partition groups awaiting export, in reverse so the JVM pulls them in first-seen order.
pub(crate) struct PartitionSplit {
    slices: Vec<RecordBatch>,
}

/// Exports the next partition group into the consumer-allocated C structs, returning false once
/// every group has been pulled.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_nextPartitionSlice<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let split = unsafe { &mut *(handle as *mut PartitionSplit) };
        match split.slices.pop() {
            Some(slice) => {
                export_record_batch(slice, out_array_address, out_schema_address);
                1
            }
            None => 0,
        }
    })
}

/// Releases a partition split handle, dropping any groups the JVM did not pull.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_closePartitionSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<PartitionSplit>(handle));
        }
    })
}

/// Reorders/selects a batch's columns to match the requested projection (by name); identity when no
/// projection was requested. Shared by the file sources so projection pushdown behaves identically.
/// A native source the JVM pulls one Arrow batch at a time, until the split is exhausted. The concrete
/// reader is hidden behind this trait so the file formats share one open/next/close bridge — the bytes
/// are read and decoded directly in the engine, never crossing into the row world.
pub(crate) trait BatchSource {
    fn next_batch(&mut self) -> Option<RecordBatch>;
}

/// A scan of one file split, driven by DataFusion's file-scan execution — the same path
/// datafusion-comet uses. DataFusion selects the row groups whose start falls in the split's byte
/// range, pushes the projection into the decode (only the wanted columns are read), and yields Arrow
/// batches, which we pull synchronously on the shared runtime. File formats differ only in the
/// `FileSource` constructed.
pub(crate) struct FileScan {
    stream: datafusion::physical_plan::SendableRecordBatchStream,
}

impl FileScan {
    fn open(
        file_source: Arc<dyn datafusion::datasource::physical_plan::FileSource>,
        path: &str,
        schema: SchemaRef,
        projection: &[String],
        range_start: i64,
        range_length: i64,
    ) -> FileScan {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::FileScanConfigBuilder;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_plan::ExecutionPlan;

        let size = std::fs::metadata(path).expect("failed to stat source file").len();
        let file =
            PartitionedFile::new_with_range(path.to_string(), size, range_start, range_start + range_length);
        // Projection as column indices in plan order; an empty projection reads every column.
        let indices: Vec<usize> = if projection.is_empty() {
            (0..schema.fields().len()).collect()
        } else {
            projection
                .iter()
                .map(|name| schema.index_of(name).expect("projected column not in source file"))
                .collect()
        };
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file(file)
            .with_projection_indices(Some(indices))
            .expect("failed to set scan projection")
            .build();
        let stream = DataSourceExec::from_data_source(config)
            .execute(0, SessionContext::new().task_ctx())
            .expect("failed to start file scan");
        FileScan { stream }
    }
}

impl BatchSource for FileScan {
    fn next_batch(&mut self) -> Option<RecordBatch> {
        runtime()
            .block_on(async { self.stream.next().await })
            .map(|batch| batch.expect("failed to read file batch"))
    }
}

/// The Arrow schema of a Parquet file, read from its footer to build the scan's file source.
pub(crate) fn parquet_file_schema(path: &str) -> SchemaRef {
    let file = std::fs::File::open(path).expect("failed to open parquet file");
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to read parquet metadata")
        .schema()
        .clone()
}

/// Opens one Parquet split — the row groups of `path` within `[range_start, range_start +
/// range_length)` — and returns an opaque handle, released with `closeSource`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_openParquet<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    projection: JObjectArray<'local>,
    range_start: jlong,
    range_length: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let path: String = env.get_string(&path).expect("failed to read path").into();
        let projection = read_strings(&mut env, &projection)
            .into_iter()
            .map(|name| name.expect("projection column name was null"))
            .collect::<Vec<_>>();
        let schema = parquet_file_schema(&path);
        let file_source = Arc::new(
            datafusion::datasource::physical_plan::ParquetSource::new(schema.clone()),
        ) as Arc<dyn datafusion::datasource::physical_plan::FileSource>;
        let source: Box<dyn BatchSource> =
            Box::new(FileScan::open(file_source, &path, schema, &projection, range_start, range_length));
        into_handle(source)
    })
}

/// Exports the next Arrow batch from the source into the consumer-allocated C structs, returning
/// true if a batch was produced and false once the directory is exhausted. Shared by every native
/// file source.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_nextBatch<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let source = unsafe { &mut *(handle as *mut Box<dyn BatchSource>) };
        match source.next_batch() {
            Some(batch) => {
                export_record_batch(batch, out_array_address, out_schema_address);
                1
            }
            None => 0,
        }
    })
}

/// Releases a native file source handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_parquet_NativeParquet_closeSource<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<Box<dyn BatchSource>>(handle));
        }
    })
}

#[cfg(test)]
mod parquet_encoder_tests {
    use super::*;
    use arrow::array::{Date32Array, Time64MicrosecondArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType};

    /// Runs batches through an encoder and returns the complete encoded file, draining through a
    /// deliberately small chunk to exercise the partial-drain path the JVM uses.
    fn encode(
        schema: SchemaRef,
        partition_columns: &[usize],
        config: &[(&str, &str)],
        batches: &[RecordBatch],
    ) -> Vec<u8> {
        let keys: Vec<String> = config.iter().map(|(k, _)| k.to_string()).collect();
        let values: Vec<String> = config.iter().map(|(_, v)| v.to_string()).collect();
        let mut encoder = ParquetEncoder::new(schema, partition_columns, &keys, &values);
        let mut file = Vec::new();
        let mut chunk = [0u8; 61];
        for batch in batches {
            encoder.write(batch);
            loop {
                let n = encoder.drain_into(&mut chunk);
                if n == 0 {
                    break;
                }
                file.extend_from_slice(&chunk[..n]);
            }
        }
        encoder.finish();
        loop {
            let n = encoder.drain_into(&mut chunk);
            if n == 0 {
                break;
            }
            file.extend_from_slice(&chunk[..n]);
        }
        file
    }

    fn read_back(file: Vec<u8>) -> (Vec<RecordBatch>, parquet::file::metadata::ParquetMetaData) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file))
            .expect("encoder output was not a readable parquet file");
        let metadata = builder.metadata().as_ref().clone();
        let batches = builder
            .build()
            .expect("failed to build reader")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches back");
        (batches, metadata)
    }

    #[test]
    fn schema_descriptor_matches_flink_shape() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, false),
            Field::new("small_dec", DataType::Decimal128(5, 2), true),
            Field::new("large_dec", DataType::Decimal128(38, 10), true),
            Field::new("d", DataType::Date32, true),
            Field::new(
                "t",
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                true,
            ),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("b", DataType::Boolean, true),
                Field::new("i8", DataType::Int8, true),
                Field::new("i16", DataType::Int16, false),
                Field::new("small_dec", DataType::Decimal128(5, 2), true),
                Field::new("large_dec", DataType::Decimal128(38, 10), true),
                Field::new("d", DataType::Date32, true),
                Field::new(
                    "t",
                    DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                    true,
                ),
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(Int8Array::from(vec![Some(1i8)])),
                Arc::new(Int16Array::from(vec![2i16])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12345i128)])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(1i128 << 100)])
                        .with_precision_and_scale(38, 10)
                        .unwrap(),
                ),
                Arc::new(Date32Array::from(vec![Some(19000)])),
                Arc::new(Time64MicrosecondArray::from(vec![Some(1_500i64)])),
                Arc::new(TimestampNanosecondArray::from(vec![Some(1_000_000_000i64)])),
            ],
        )
        .unwrap();
        let file = encode(schema, &[], &[], &[batch]);
        let (_, metadata) = read_back(file);

        let descriptor = metadata.file_metadata().schema_descr();
        assert_eq!(descriptor.root_schema().name(), "flink_schema");
        // No embedded Arrow schema: the forced descriptor is the only source of truth, as in Flink.
        assert!(metadata.file_metadata().key_value_metadata().is_none());

        let leaf = |index: usize| descriptor.column(index).self_type_ptr();
        assert_eq!(leaf(0).get_physical_type(), PhysicalType::BOOLEAN);
        assert_eq!(
            leaf(1).get_basic_info().converted_type(),
            ConvertedType::INT_8
        );
        assert_eq!(leaf(2).get_basic_info().repetition(), Repetition::REQUIRED);
        // Flink writes every decimal as minimal-length FIXED_LEN_BYTE_ARRAY, never INT32/INT64.
        let small_dec = leaf(3);
        assert_eq!(
            small_dec.get_physical_type(),
            PhysicalType::FIXED_LEN_BYTE_ARRAY
        );
        match small_dec.as_ref() {
            parquet::schema::types::Type::PrimitiveType { type_length, .. } => {
                assert_eq!(*type_length, 3)
            }
            _ => panic!("expected primitive"),
        }
        let large_dec = leaf(4);
        assert_eq!(
            large_dec.get_physical_type(),
            PhysicalType::FIXED_LEN_BYTE_ARRAY
        );
        match large_dec.as_ref() {
            parquet::schema::types::Type::PrimitiveType { type_length, .. } => {
                assert_eq!(*type_length, 16)
            }
            _ => panic!("expected primitive"),
        }
        assert_eq!(
            leaf(5).get_basic_info().logical_type_ref(),
            Some(&LogicalType::Date)
        );
        assert_eq!(
            leaf(6).get_basic_info().logical_type_ref(),
            Some(&LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MILLIS
            })
        );
        assert_eq!(leaf(6).get_physical_type(), PhysicalType::INT32);
        assert_eq!(
            leaf(7).get_basic_info().logical_type_ref(),
            Some(&LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS
            })
        );
    }

    #[test]
    fn timestamp_units_floor_pre_epoch_values() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        )]));
        // -1.5ms before the epoch: Flink's TimestampData(millis=-2, nanoOfMilli=500_000) floors to
        // -2ms and -1500us; truncation toward zero would give -1 and -1500 respectively.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampNanosecondArray::from(vec![Some(
                -1_500_000i64,
            )]))],
        )
        .unwrap();

        let (batches, _) = read_back(encode(
            schema.clone(),
            &[],
            &[("timestamp.unit", "millis")],
            &[batch.clone()],
        ));
        let millis = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(millis.value(0), -2);

        let (batches, _) = read_back(encode(
            schema.clone(),
            &[],
            &[("timestamp.unit", "micros")],
            &[batch.clone()],
        ));
        let micros = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(micros.value(0), -1_500);

        let (batches, _) = read_back(encode(
            schema,
            &[],
            &[("timestamp.unit", "nanos")],
            &[batch],
        ));
        let nanos = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(nanos.value(0), -1_500_000);
    }

    #[test]
    fn decimal_values_roundtrip_through_flba() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("small", DataType::Decimal128(5, 2), true),
            Field::new("large", DataType::Decimal128(20, 0), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![Some(-12345i128), None, Some(99999i128)])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(-(1i128 << 64)), Some(1i128 << 64), None])
                        .with_precision_and_scale(20, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let (batches, _) = read_back(encode(schema, &[], &[], &[batch]));
        let small = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(small.value(0), -12345);
        assert!(small.is_null(1));
        assert_eq!(small.value(2), 99999);
        let large = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(large.value(0), -(1i128 << 64));
        assert_eq!(large.value(1), 1i128 << 64);
        assert!(large.is_null(2));
    }

    #[test]
    fn time_narrows_to_millis_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Time64MicrosecondArray::from(vec![
                Some(1_999i64),
                None,
            ]))],
        )
        .unwrap();
        let (batches, _) = read_back(encode(schema, &[], &[], &[batch]));
        let times = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Time32MillisecondArray>()
            .unwrap();
        assert_eq!(times.value(0), 1);
        assert!(times.is_null(1));
    }

    #[test]
    fn compression_defaults_snappy_and_honors_zstd_level() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>()))],
        )
        .unwrap();

        let (_, metadata) = read_back(encode(schema.clone(), &[], &[], &[batch.clone()]));
        assert_eq!(
            metadata.row_group(0).column(0).compression(),
            parquet::basic::Compression::SNAPPY
        );

        let (_, metadata) = read_back(encode(
            schema,
            &[],
            &[("compression", "ZSTD"), ("compression.zstd.level", "7")],
            &[batch],
        ));
        // The footer records only the codec; levels are asserted on the parsed config below.
        assert!(matches!(
            metadata.row_group(0).column(0).compression(),
            parquet::basic::Compression::ZSTD(_)
        ));

        let config = EncoderConfig::parse(
            &["compression".into(), "compression.zstd.level".into()],
            &["ZSTD".into(), "7".into()],
        );
        assert_eq!(
            config.compression,
            parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(7).unwrap())
        );
        // Unset levels resolve to the Flink-effective defaults (zstd 3, gzip 6), not parquet-rs's.
        let config = EncoderConfig::parse(&["compression".into()], &["ZSTD".into()]);
        assert_eq!(
            config.compression,
            parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap())
        );
        let config = EncoderConfig::parse(&["compression".into()], &["GZIP".into()]);
        assert_eq!(
            config.compression,
            parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::try_new(6).unwrap())
        );
    }

    #[test]
    fn row_groups_split_by_bytes_not_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        // The byte limit engages once buffered rows exist to estimate row width from, i.e. from the
        // second batch onward — the streaming sink always feeds a stream of batches, so split the
        // rows the way the operator would.
        let batches: Vec<RecordBatch> = (0..20)
            .map(|chunk| {
                let values: Vec<String> = (chunk * 1000..(chunk + 1) * 1000)
                    .map(|i| format!("row-{i}-padding-padding"))
                    .collect();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(
                        values.iter().map(String::as_str).collect::<Vec<_>>(),
                    ))],
                )
                .unwrap()
            })
            .collect();
        // A tiny block size must split; the parquet-rs default 1M-row cap must not be the trigger.
        let (read, metadata) = read_back(encode(
            schema.clone(),
            &[],
            &[("block.size", "65536"), ("enable.dictionary", "false")],
            &batches,
        ));
        assert!(
            metadata.num_row_groups() > 1,
            "expected byte-driven row group splits"
        );
        assert_eq!(
            read.iter().map(RecordBatch::num_rows).sum::<usize>(),
            20_000
        );

        let (_, metadata) = read_back(encode(schema, &[], &[], &batches));
        assert_eq!(metadata.num_row_groups(), 1);
    }

    #[test]
    fn partition_columns_are_projected_out() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("p", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("part")])),
                Arc::new(Int64Array::from(vec![Some(2i64)])),
            ],
        )
        .unwrap();
        let (batches, metadata) = read_back(encode(schema, &[1], &[], &[batch]));
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "a");
        assert_eq!(batches[0].schema().field(1).name(), "b");
    }

    #[test]
    fn empty_file_is_valid_parquet() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let (batches, metadata) = read_back(encode(schema, &[], &[], &[]));
        assert!(batches.is_empty());
        assert_eq!(metadata.file_metadata().num_rows(), 0);
    }

    #[test]
    fn statistics_match_parquet_mr_defaults() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let long_value = "x".repeat(200);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                Some(long_value.as_str()),
                Some("a"),
            ]))],
        )
        .unwrap();
        let (_, metadata) = read_back(encode(schema, &[], &[], &[batch]));
        let column = metadata.row_group(0).column(0);
        let statistics = column.statistics().expect("chunk statistics missing");
        // parquet-mr leaves chunk-level min/max untruncated; parquet-rs would truncate to 64 bytes
        // by default, so the 200-byte max must have survived intact.
        assert_eq!(statistics.max_bytes_opt().expect("max missing").len(), 200);
    }
}

#[cfg(test)]
mod partition_split_tests {
    use super::*;

    fn batch(keys: Vec<Option<&str>>, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, true),
                Field::new("v", DataType::Int32, false),
            ])),
            vec![Arc::new(StringArray::from(keys)), Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    }

    fn key_of(slice: &RecordBatch, row: usize) -> Option<String> {
        let keys = slice.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        if keys.is_null(row) { None } else { Some(keys.value(row).to_string()) }
    }

    #[test]
    fn groups_in_first_seen_order_with_null_key_group() {
        let batch = batch(
            vec![Some("b"), None, Some("a"), Some("b"), None, Some("b")],
            vec![1, 2, 3, 4, 5, 6],
        );
        let slices = split_by_partition_columns(&batch, &[0]);
        assert_eq!(slices.len(), 3);

        assert_eq!(key_of(&slices[0], 0), Some("b".to_string()));
        let values = slices[0].column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[1, 4, 6]);

        assert_eq!(key_of(&slices[1], 0), None);
        let values = slices[1].column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[2, 5]);

        assert_eq!(key_of(&slices[2], 0), Some("a".to_string()));
        let values = slices[2].column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[3]);

        // Every group is single-keyed: the JVM routes each slice by its row 0.
        for slice in &slices {
            for row in 1..slice.num_rows() {
                assert_eq!(key_of(slice, row), key_of(slice, 0));
            }
        }
    }

    #[test]
    fn multi_column_keys_group_together() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Utf8, true),
                Field::new("k2", DataType::Int32, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("x"), Some("y"), Some("x")])),
                Arc::new(Int32Array::from(vec![1, 2, 1, 1])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30, 40])),
            ],
        )
        .unwrap();
        let slices = split_by_partition_columns(&batch, &[0, 1]);
        assert_eq!(slices.len(), 3);
        let values = slices[0].column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[10, 40]); // (x, 1)
        let values = slices[1].column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[20]); // (x, 2)
        let values = slices[2].column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[30]); // (y, 1)
    }

    #[test]
    fn single_key_batch_passes_through_unpermuted() {
        let source = batch(vec![Some("only"), Some("only")], vec![1, 2]);
        let slices = split_by_partition_columns(&source, &[0]);
        assert_eq!(slices.len(), 1);
        // The fast path must hand back the same underlying buffers, not a permuted copy.
        assert_eq!(
            source.column(1).to_data().buffers()[0].as_ptr(),
            slices[0].column(1).to_data().buffers()[0].as_ptr()
        );
        assert_eq!(slices[0].num_rows(), 2);
    }

    #[test]
    fn sliced_groups_encode_correctly() {
        // A permuted group is a sliced batch (nonzero offset); it must encode into a parquet file
        // whose rows are exactly the group's, proving the slice survives the encoder path.
        let source = batch(vec![Some("b"), Some("a"), Some("b")], vec![1, 2, 3]);
        let slices = split_by_partition_columns(&source, &[0]);
        let mut encoder = ParquetEncoder::new(source.schema(), &[0], &[], &[]);
        encoder.write(&slices[1]);
        encoder.finish();
        let mut file = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = encoder.drain_into(&mut chunk);
            if n == 0 {
                break;
            }
            file.extend_from_slice(&chunk[..n]);
        }
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(file),
        )
        .unwrap()
        .build()
        .unwrap();
        let read: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(read[0].num_columns(), 1);
        let values = read[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[2]);
    }
}
