use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{DataContentType, DataFileFormat, PrimitiveType, Schema, Type};
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
    #[serde(default)]
    pub first_row_id: Option<i64>,
    #[serde(default)]
    pub data_sequence_number: Option<i64>,
    #[serde(default)]
    pub columnar_equality_sets: bool,
    pub case_sensitive: bool,
    #[serde(default)]
    pub metadata: crate::task_metadata::TaskMetadata,
}

impl ScanTask {
    fn task(self) -> Result<FileScanTask> {
        let schema = Arc::new(self.schema);
        let metadata = self.metadata.resolve(&schema, self.case_sensitive)?;
        FileScanTask::builder()
            .with_file_size_in_bytes(self.file_size)
            .with_start(self.start)
            .with_length(self.length)
            .with_data_file_path(self.path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(self.project_ids)
            .with_predicate(metadata.predicate)
            .with_deletes(self.deletes)
            .with_partition(metadata.partition)
            .with_partition_spec(metadata.partition_spec)
            .with_name_mapping(metadata.name_mapping)
            .with_case_sensitive(self.case_sensitive)
            .with_first_row_id(self.first_row_id)
            .with_data_sequence_number(self.data_sequence_number)
            .build()
    }
}

pub(crate) struct Reader {
    stream: ArrowRecordBatchStream,
    skip: u64,
    output_schema: Option<SchemaRef>,
    equality_tasks: Vec<(FileIO, FileScanTask, Vec<i32>)>,
    equality_schema: Option<SchemaRef>,
    equality_sets: Vec<crate::equality::EqualitySet>,
    pub(crate) retained_bytes: usize,
}

impl Reader {
    pub(crate) fn new(
        io: FileIO,
        mut task: ScanTask,
        batch_rows: usize,
        skip: u64,
    ) -> Result<Self> {
        if batch_rows == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Arrow batch size must be positive",
            ));
        }
        let mut equality_deletes = Vec::new();
        if task.columnar_equality_sets {
            task.deletes.retain(|delete| {
                let ids = delete.equality_ids.as_deref().unwrap_or_default();
                let eligible = delete.file_type == DataContentType::EqualityDeletes
                    && !ids.is_empty()
                    && ids.iter().all(|id| {
                        crate::schema::root_id(&task.schema, *id).is_some()
                            && task.schema.field_by_id(*id).is_some_and(|field| {
                                matches!(
                                    field.field_type.as_ref(),
                                    Type::Primitive(
                                        PrimitiveType::Int
                                            | PrimitiveType::Long
                                            | PrimitiveType::String
                                    )
                                )
                            })
                    });
                if eligible {
                    equality_deletes.push(delete.clone());
                }
                !eligible
            });
        }
        for delete in &equality_deletes {
            for id in delete.equality_ids.as_ref().unwrap() {
                let root = crate::schema::root_id(&task.schema, *id).unwrap();
                if !task.project_ids.contains(&root) {
                    task.project_ids.push(root);
                }
            }
        }
        let equality_schema = if equality_deletes.is_empty() {
            None
        } else {
            Some(crate::schema::projected_schema(
                &task.schema,
                &task.project_ids,
            )?)
        };
        let reader = ArrowReaderBuilder::new(io.clone(), Runtime::new(runtime()))
            .with_batch_size(batch_rows)
            .with_data_file_concurrency_limit(1)
            .with_row_selection_enabled(false)
            .build();
        let task = task.task()?;
        let equality_tasks = equality_deletes
            .into_iter()
            .map(|delete| {
                let ids = delete.equality_ids.unwrap();
                let mut roots = Vec::new();
                for id in &ids {
                    let root = crate::schema::root_id(task.schema(), *id).unwrap();
                    if !roots.contains(&root) {
                        roots.push(root);
                    }
                }
                let scan = FileScanTask::builder()
                    .with_file_size_in_bytes(delete.file_size_in_bytes)
                    .with_start(0)
                    .with_length(delete.file_size_in_bytes)
                    .with_data_file_path(delete.file_path)
                    .with_data_file_format(DataFileFormat::Parquet)
                    .with_schema(task.schema_ref())
                    .with_project_field_ids(roots)
                    .with_case_sensitive(task.case_sensitive())
                    .with_name_mapping(task.name_mapping().cloned())
                    .build()?;
                Ok((io.clone(), scan, ids))
            })
            .collect::<Result<Vec<_>>>()?;
        let tasks = futures::stream::once(async move { crate::scan_range::tasks(&io, task).await })
            .map_ok(|tasks| futures::stream::iter(tasks.into_iter().map(Ok)))
            .try_flatten()
            .boxed();
        Ok(Self {
            stream: reader.read(tasks)?.stream(),
            skip,
            output_schema: None,
            equality_tasks,
            equality_schema,
            equality_sets: Vec::new(),
            retained_bytes: 0,
        })
    }

    pub(crate) fn with_output_schema(mut self, schema: SchemaRef) -> Self {
        self.output_schema = Some(schema);
        self
    }

    pub(crate) async fn next(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.stream.try_next().await? {
            let batch = match &self.equality_schema {
                Some(schema) => {
                    let mut fields = schema.fields().to_vec();
                    let extra = batch
                        .schema()
                        .fields()
                        .iter()
                        .filter(|field| {
                            !fields
                                .iter()
                                .any(|expected| expected.name() == field.name())
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    fields.extend(extra);
                    crate::schema::align(batch, Arc::new(arrow::datatypes::Schema::new(fields)))
                        .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))?
                }
                None => batch,
            };
            for (io, task, ids) in std::mem::take(&mut self.equality_tasks) {
                if let Some(set) =
                    crate::equality::EqualitySet::load(io, task, &batch.schema(), &ids).await?
                {
                    self.retained_bytes += set.retained_bytes;
                    self.equality_sets.push(set);
                }
            }
            let mut batch = batch;
            for set in &self.equality_sets {
                batch = set.apply(batch)?;
            }
            if let Some(schema) = &self.output_schema {
                batch = crate::schema::align(batch, schema.clone())
                    .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))?;
            }
            if self.skip >= batch.num_rows() as u64 {
                self.skip -= batch.num_rows() as u64;
                continue;
            }
            let skip = std::mem::take(&mut self.skip) as usize;
            let batch = batch.slice(skip, batch.num_rows() - skip);
            return Ok(Some(batch));
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
                        first_row_id: None,
                        data_sequence_number: None,
                        columnar_equality_sets: false,
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
                first_row_id: None,
                data_sequence_number: None,
                columnar_equality_sets: false,
                case_sensitive: true,
                metadata: Default::default(),
            };
            let mut reader = Reader::new(io, task, 13, 102).unwrap();
            assert!(reader.next().await.is_err());
        });
    }
}
