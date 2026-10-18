use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{DataFileFormat, Schema};
use iceberg::{Error, ErrorKind, Result, Runtime};
use serde::Deserialize;
use std::sync::{Arc, OnceLock};

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("streamfusion-iceberg-io")
            .enable_all()
            .build()
            .expect("Iceberg IO runtime")
    })
}

/// Explicit metadata boundary, independent of Java's internal task serializer.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScanTask {
    pub schema: Schema,
    pub path: String,
    pub file_size: u64,
    pub start: u64,
    pub length: u64,
    pub project_ids: Vec<i32>,
    pub deletes: Vec<FileScanTaskDeleteFile>,
    pub case_sensitive: bool,
    #[serde(default)]
    pub metadata: crate::task_metadata::TaskMetadata,
}

impl ScanTask {
    fn task(self) -> Result<FileScanTask> {
        let mut task = FileScanTask {
            file_size_in_bytes: self.file_size,
            start: self.start,
            length: self.length,
            record_count: None,
            data_file_path: self.path,
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(self.schema),
            project_field_ids: self.project_ids,
            predicate: None,
            deletes: self.deletes,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: self.case_sensitive,
        };
        self.metadata.apply(&mut task)?;
        Ok(task)
    }
}

pub(crate) struct Reader {
    stream: ArrowRecordBatchStream,
    skip: u64,
    output_schema: Option<SchemaRef>,
}

impl Reader {
    pub(crate) fn new(io: FileIO, task: ScanTask, batch_rows: usize, skip: u64) -> Result<Self> {
        if batch_rows == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Arrow batch size must be positive",
            ));
        }
        let reader = ArrowReaderBuilder::new(io.clone(), Runtime::new(runtime()))
            .with_batch_size(batch_rows)
            .with_data_file_concurrency_limit(1)
            .with_row_selection_enabled(false)
            .build();
        let task = task.task()?;
        let tasks = futures::stream::once(async move { crate::scan_range::tasks(&io, task).await })
            .map_ok(|tasks| futures::stream::iter(tasks.into_iter().map(Ok)))
            .try_flatten()
            .boxed();
        Ok(Self {
            stream: reader.read(tasks)?.stream(),
            skip,
            output_schema: None,
        })
    }

    pub(crate) fn with_output_schema(mut self, schema: SchemaRef) -> Self {
        self.output_schema = Some(schema);
        self
    }

    pub(crate) async fn next(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.stream.try_next().await? {
            if self.skip >= batch.num_rows() as u64 {
                self.skip -= batch.num_rows() as u64;
                continue;
            }
            let skip = std::mem::take(&mut self.skip) as usize;
            let batch = batch.slice(skip, batch.num_rows() - skip);
            let Some(schema) = &self.output_schema else {
                return Ok(Some(batch));
            };
            if batch.num_columns() != schema.fields().len() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Iceberg output projection mismatch",
                ));
            }
            let columns = batch
                .columns()
                .iter()
                .zip(schema.fields())
                .map(|(column, field)| {
                    if column.data_type() == field.data_type() {
                        Ok(column.clone())
                    } else if streamfusion_bridge::timestamp::is_timestamp(column.data_type())
                        && streamfusion_bridge::timestamp::is_timestamp(field.data_type())
                    {
                        streamfusion_bridge::timestamp::cast_timestamp(column, field.data_type())
                    } else {
                        arrow::compute::cast(column, field.data_type())
                    }
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))?;
            let output = RecordBatch::try_new_with_options(
                schema.clone(),
                columns,
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))?;
            return Ok(Some(output));
        }
        if self.skip != 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Restored Iceberg record offset exceeds task",
            ));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    #[test]
    fn restored_logical_offsets_do_not_depend_on_batch_size() {
        let schema = Schema::builder()
            .with_fields([
                NestedField::required(17, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(38, "value", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        let input = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..101)) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from_iter(
                    (0..101).map(|i| (i % 7 != 0).then(|| format!("value-{i}"))),
                )) as ArrayRef,
            ),
        ])
        .unwrap();
        let bytes = crate::bench::encode(&schema, &input, &(0..101).collect::<Vec<_>>());
        runtime().block_on(async {
            let io = FileIO::new_with_memory();
            io.new_output("task.parquet")
                .unwrap()
                .write(bytes.clone())
                .await
                .unwrap();
            for batch_rows in [1, 13, 128] {
                for skip in [0, 1, 13, 40, 100, 101] {
                    let task = ScanTask {
                        schema: schema.clone(),
                        path: "task.parquet".into(),
                        file_size: bytes.len() as u64,
                        start: 0,
                        length: bytes.len() as u64,
                        project_ids: vec![17, 38],
                        deletes: vec![],
                        case_sensitive: true,
                        metadata: Default::default(),
                    };
                    let mut reader = Reader::new(io.clone(), task, batch_rows, skip).unwrap();
                    let mut offset = skip as usize;
                    while let Some(batch) = reader.next().await.unwrap() {
                        assert!(batch.num_rows() <= batch_rows);
                        let actual = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        for i in 0..actual.len() {
                            assert_eq!(offset as i64, actual.value(i));
                            assert_eq!(offset % 7 == 0, batch.column(1).is_null(i));
                            offset += 1;
                        }
                    }
                    assert_eq!(offset, 101);
                }
            }
            let task = ScanTask {
                schema,
                path: "task.parquet".into(),
                file_size: bytes.len() as u64,
                start: 0,
                length: bytes.len() as u64,
                project_ids: vec![38],
                deletes: vec![],
                case_sensitive: true,
                metadata: Default::default(),
            };
            let mut reader = Reader::new(io, task, 13, 102).unwrap();
            assert!(reader.next().await.is_err());
        });
    }
}
