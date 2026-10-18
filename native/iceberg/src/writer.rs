use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{cast, take};
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use iceberg::io::FileWrite;
use iceberg::io::OutputFile;
use iceberg::spec::Schema as IcebergSchema;
use parquet::arrow::async_writer::{AsyncArrowWriter, AsyncFileWriter};
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriterSettings {
    pub compression: String,
    pub compression_level: Option<i32>,
    pub row_group_bytes: usize,
    pub page_bytes: usize,
    pub page_rows: usize,
    pub dictionary_bytes: usize,
    pub dictionary_enabled: bool,
    pub version: u8,
}

impl WriterSettings {
    fn properties(&self) -> WriterProperties {
        let compression = match self.compression.as_str() {
            "uncompressed" => Compression::UNCOMPRESSED,
            "snappy" => Compression::SNAPPY,
            "zstd" => Compression::ZSTD(
                ZstdLevel::try_new(self.compression_level.unwrap_or(3)).expect("Zstd level"),
            ),
            "gzip" => Compression::GZIP(
                GzipLevel::try_new(self.compression_level.unwrap_or(6) as u32).expect("Gzip level"),
            ),
            "lz4" => Compression::LZ4_RAW,
            other => panic!("Unadmitted Iceberg codec: {other}"),
        };
        WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(Some(self.row_group_bytes))
            .set_data_page_size_limit(self.page_bytes)
            .set_data_page_row_count_limit(self.page_rows)
            .set_dictionary_page_size_limit(self.dictionary_bytes)
            .set_dictionary_enabled(self.dictionary_enabled)
            .set_writer_version(match self.version {
                1 => WriterVersion::PARQUET_1_0,
                2 => WriterVersion::PARQUET_2_0,
                _ => panic!("Unadmitted Parquet writer version"),
            })
            .build()
    }
}

pub(crate) struct Encoder {
    pub input_schema: SchemaRef,
    write_schema: SchemaRef,
    writer: Option<AsyncArrowWriter<HostOutput>>,
    nan_columns: Vec<(usize, i32)>,
    nan_counts: HashMap<i32, u64>,
    metadata_bytes: usize,
    flushed_groups: usize,
}

#[derive(Serialize)]
pub(crate) struct FileResult {
    pub row_count: u64,
    pub size: u64,
    pub nan_counts: std::collections::HashMap<i32, u64>,
    pub split_offsets: Vec<i64>,
}

impl Encoder {
    pub(crate) async fn new(
        input_schema: SchemaRef,
        schema: IcebergSchema,
        settings: WriterSettings,
        output: OutputFile,
    ) -> Self {
        let schema = Arc::new(schema);
        let write_schema: SchemaRef =
            Arc::new(schema.as_ref().try_into().expect("Iceberg Arrow schema"));
        let nan_columns = write_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                matches!(
                    field.data_type(),
                    arrow::datatypes::DataType::Float32 | arrow::datatypes::DataType::Float64
                )
                .then(|| {
                    (
                        index,
                        field.metadata()["PARQUET:field_id"]
                            .parse::<i32>()
                            .expect("Iceberg field id"),
                    )
                })
            })
            .collect::<Vec<_>>();
        let nan_counts = nan_columns.iter().map(|(_, id)| (*id, 0)).collect();
        let writer = AsyncArrowWriter::try_new(
            HostOutput(output.writer().await.expect("Iceberg output writer")),
            write_schema.clone(),
            Some(settings.properties()),
        )
        .expect("Iceberg Parquet writer");
        Self {
            input_schema,
            write_schema,
            writer: Some(writer),
            nan_columns,
            nan_counts,
            metadata_bytes: 0,
            flushed_groups: 0,
        }
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch, selected: Vec<u32>) {
        if selected.is_empty() {
            return;
        }
        let contiguous = selected.windows(2).all(|w| w[1] == w[0] + 1);
        let start = selected[0] as usize;
        let count = selected.len();
        let indices = (!contiguous).then(|| UInt32Array::from(selected));
        let columns = self
            .write_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let column = if contiguous {
                    batch.column(i).slice(start, count)
                } else {
                    take(
                        batch.column(i),
                        indices.as_ref().expect("Sparse selection"),
                        None,
                    )
                    .expect("Iceberg Arrow selection")
                };
                if column.data_type() == field.data_type() {
                    column
                } else if streamfusion_bridge::timestamp::is_timestamp(column.data_type())
                    && streamfusion_bridge::timestamp::is_timestamp(field.data_type())
                {
                    streamfusion_bridge::timestamp::cast_timestamp(&column, field.data_type())
                        .expect("Iceberg timestamp column conversion")
                } else {
                    cast(&column, field.data_type()).expect("Iceberg physical column conversion")
                }
            })
            .collect::<Vec<_>>();
        let selected = RecordBatch::try_new(self.write_schema.clone(), columns)
            .expect("Iceberg selected record batch");
        for (index, id) in &self.nan_columns {
            let column = selected.column(*index);
            let count = if let Some(values) =
                column.as_any().downcast_ref::<arrow::array::Float32Array>()
            {
                values
                    .iter()
                    .flatten()
                    .filter(|value| value.is_nan())
                    .count()
            } else {
                column
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .expect("Iceberg float column")
                    .iter()
                    .flatten()
                    .filter(|value| value.is_nan())
                    .count()
            };
            *self.nan_counts.get_mut(id).expect("Iceberg NaN count") += count as u64;
        }
        let writer = self.writer.as_mut().expect("Iceberg writer closed");
        writer
            .write(&selected)
            .await
            .expect("Iceberg native Parquet write");
        for group in &writer.flushed_row_groups()[self.flushed_groups..] {
            self.metadata_bytes += 256
                + group
                    .columns()
                    .iter()
                    .map(|column| {
                        let bounds = column.statistics().map_or(0, |stats| {
                            stats.min_bytes_opt().map_or(0, <[u8]>::len)
                                + stats.max_bytes_opt().map_or(0, <[u8]>::len)
                        });
                        1024 + bounds
                    })
                    .sum::<usize>();
        }
        self.flushed_groups = writer.flushed_row_groups().len();
    }

    pub(crate) fn size(&self) -> usize {
        let writer = self.writer.as_ref().expect("Iceberg writer closed");
        writer.bytes_written() + writer.in_progress_size()
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        self.writer
            .as_ref()
            .map_or(0, |writer| writer.memory_size())
            + self.metadata_bytes
            + (1 << 20)
    }

    pub(crate) async fn finish(&mut self) -> FileResult {
        let mut writer = self.writer.take().expect("Iceberg writer closed");
        let metadata = writer
            .finish()
            .await
            .expect("Iceberg native Parquet finalization");
        FileResult {
            row_count: metadata.file_metadata().num_rows() as u64,
            size: writer.bytes_written() as u64,
            nan_counts: std::mem::take(&mut self.nan_counts),
            split_offsets: metadata
                .row_groups()
                .iter()
                .filter_map(|group| group.file_offset())
                .collect(),
        }
    }
}

struct HostOutput(Box<dyn FileWrite>);

impl AsyncFileWriter for HostOutput {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .write(bytes)
                .await
                .map_err(|failure| parquet::errors::ParquetError::External(Box::new(failure)))
        })
    }
    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .close()
                .await
                .map_err(|failure| parquet::errors::ParquetError::External(Box::new(failure)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use iceberg::io::FileIO;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn bytes_and_statistics_match_the_released_iceberg_writer() {
        use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
        use iceberg::writer::CurrentFileStatus;
        futures::executor::block_on(async {
            let schema = IcebergSchema::builder()
                .with_fields([
                    NestedField::required(17, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(38, "value", Type::Primitive(PrimitiveType::Float))
                        .into(),
                    NestedField::optional(51, "text", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap();
            let input_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float32, true),
                Field::new("text", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                input_schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..1057)),
                    Arc::new(arrow::array::Float32Array::from_iter((0..1057).map(
                        |i| match i % 7 {
                            0 => None,
                            1 => Some(f32::NAN),
                            2 => Some(-0.0),
                            3 => Some(f32::INFINITY),
                            _ => Some(i as f32),
                        },
                    ))),
                    Arc::new(StringArray::from_iter(
                        (0..1057).map(|i| (i % 5 != 0).then(|| format!("é-value-{i}"))),
                    )),
                ],
            )
            .unwrap();
            let rows = (0..1057u32).filter(|i| i % 3 != 1).collect::<Vec<_>>();
            for (codec, version) in [("uncompressed", 1), ("zstd", 2), ("gzip", 1), ("snappy", 2)] {
                for dictionary in [false, true] {
                    let settings = WriterSettings {
                        compression: codec.into(),
                        compression_level: None,
                        row_group_bytes: 4096,
                        page_bytes: 1024,
                        page_rows: 64,
                        dictionary_bytes: 1024,
                        dictionary_enabled: dictionary,
                        version,
                    };
                    let properties = settings.properties();
                    let io = FileIO::new_with_memory();
                    let mut native = Encoder::new(
                        input_schema.clone(),
                        schema.clone(),
                        settings,
                        io.new_output("native").unwrap(),
                    )
                    .await;
                    let mut released =
                        ParquetWriterBuilder::new(properties, Arc::new(schema.clone()))
                            .build(io.new_output("released").unwrap())
                            .await
                            .unwrap();
                    let indices = UInt32Array::from(rows.clone());
                    let selected = RecordBatch::try_new(
                        native.write_schema.clone(),
                        batch
                            .columns()
                            .iter()
                            .map(|column| take(column, &indices, None).unwrap())
                            .collect(),
                    )
                    .unwrap();
                    native.write(&batch, rows.clone()).await;
                    released.write(&selected).await.unwrap();
                    assert_eq!(native.size(), released.current_written_size());
                    assert!(native.memory_bytes() >= 1 << 20);
                    let actual = native.finish().await;
                    let expected = released
                        .close()
                        .await
                        .unwrap()
                        .pop()
                        .unwrap()
                        .build()
                        .unwrap();
                    assert_eq!(actual.row_count, expected.record_count());
                    assert_eq!(actual.nan_counts, *expected.nan_value_counts());
                    assert_eq!(
                        actual.split_offsets.as_slice(),
                        expected.split_offsets().unwrap()
                    );
                    assert_eq!(
                        io.new_input("native").unwrap().read().await.unwrap(),
                        io.new_input("released").unwrap().read().await.unwrap(),
                        "{codec}, dictionary={dictionary}"
                    );
                }
            }
        });
    }

    #[test]
    fn selections_preserve_order_nulls_ids_and_metrics() {
        futures::executor::block_on(async {
            let schema = IcebergSchema::builder()
                .with_fields([
                    NestedField::optional(17, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(38, "value", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap();
            let input_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("value", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                input_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
                    Arc::new(StringArray::from(vec![Some("first"), Some("second"), None])),
                ],
            )
            .unwrap();
            let settings = WriterSettings {
                compression: "zstd".into(),
                compression_level: None,
                row_group_bytes: 128 << 20,
                page_bytes: 1 << 20,
                page_rows: 20000,
                dictionary_bytes: 2 << 20,
                dictionary_enabled: true,
                version: 1,
            };
            let io = FileIO::new_with_memory();
            let mut writer = Encoder::new(
                input_schema,
                schema,
                settings,
                io.new_output("file").unwrap(),
            )
            .await;
            writer.write(&batch, vec![]).await;
            writer.write(&batch, vec![2, 0, 2]).await;
            writer.write(&batch, vec![1]).await;
            assert!(writer.size() > 0);
            let result = writer.finish().await;
            assert_eq!(result.row_count, 4);
            let bytes = io.new_input("file").unwrap().read().await.unwrap();
            assert_eq!(bytes.len() as u64, result.size);
            assert_eq!(result.split_offsets.len(), 1);
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let columns = builder.metadata().file_metadata().schema_descr().columns();
            assert_eq!(columns[0].self_type().get_basic_info().id(), 17);
            assert_eq!(columns[1].self_type().get_basic_info().id(), 38);
            let rows = builder.build().unwrap().next().unwrap().unwrap();
            let ids = rows
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(
                ids.iter().collect::<Vec<_>>(),
                vec![Some(30), Some(10), Some(30), None]
            );
            let strings = rows
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![None, Some("first"), None, Some("second")]
            );
            assert_eq!(strings.null_count(), 2);
        });
    }
}
