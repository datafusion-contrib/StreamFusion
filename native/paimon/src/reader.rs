use arrow::array::{ArrayRef, BooleanArray, Int8Array, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::StreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::resource::ResourceContext;
use paimon::spec::{
    BinaryRow, DataField, DataFileMeta, DataType, TableSchema, VarCharType, ROW_KIND_FIELD_ID,
    ROW_KIND_FIELD_NAME,
};
use paimon::table::{ArrowRecordBatchStream, AuditLogRead, DataSplit, Table};
use serde::Deserialize;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Task {
    schema: TableSchema,
    projection: Vec<DataField>,
    location: String,
    partition: Vec<u8>,
    bucket: i32,
    snapshot: i64,
    bucket_path: String,
    streaming: bool,
    raw: bool,
    files: Vec<File>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct File {
    name: String,
    size: i64,
    rows: i64,
    schema: i64,
    level: i32,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    min_sequence: i64,
    max_sequence: i64,
    deletes: Option<i64>,
    path: String,
}

impl File {
    fn metadata(self) -> DataFileMeta {
        // Java retains query filters; no data predicates use these omitted pruning statistics.
        let stats = serde_json::json!({"_MIN_VALUES": BinaryRow::new(0).to_serialized_bytes(), "_MAX_VALUES": BinaryRow::new(0).to_serialized_bytes(), "_NULL_COUNTS": []});
        DataFileMeta {
            file_name: self.name,
            file_size: self.size,
            row_count: self.rows,
            min_key: self.min_key,
            max_key: self.max_key,
            key_stats: serde_json::from_value(stats.clone()).expect("Unpruned key stats"),
            value_stats: serde_json::from_value(stats).expect("Unpruned value stats"),
            min_sequence_number: self.min_sequence,
            max_sequence_number: self.max_sequence,
            schema_id: self.schema,
            level: self.level,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: self.deletes,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: Some(self.path),
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        }
    }
}

pub(crate) struct Reader {
    stream: ArrowRecordBatchStream,
    runtime: Runtime,
    output: SchemaRef,
    kinds: bool,
    filter_deletes: bool,
    resources: ResourceContext,
}

impl Reader {
    pub fn new(
        io: FileIO,
        json: &str,
        output: SchemaRef,
        rows: usize,
        budget: usize,
        kinds: bool,
    ) -> Self {
        let task: Task = serde_json::from_str(json).expect("Paimon read descriptor");
        let filter_deletes = kinds && !task.raw && !task.streaming;
        let mut resources = ResourceContext::builder();
        if !task.raw {
            resources = resources.memory_limit(budget);
        }
        let resources = resources.build().expect("Paimon read resources");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Paimon reader runtime");
        let stream = runtime.block_on(async {
            let mut options = task.schema.options().clone();
            options.insert("read.batch-size".into(), rows.to_string());
            let schema = task.schema.copy_with_options(options);
            let table = Table::from_resolved_schema(
                io,
                Identifier::new("streamfusion", "source"),
                task.location,
                schema,
                "main",
            )
            .expect("Resolved Paimon table");
            let split = DataSplit::builder()
                .with_snapshot(task.snapshot)
                .with_partition(
                    BinaryRow::from_serialized_bytes(&task.partition).expect("Paimon partition"),
                )
                // Java append tables use -1; this reader has explicit file paths, no bucket lookup.
                .with_bucket(task.bucket.max(0))
                .with_bucket_path(task.bucket_path)
                .with_data_files(task.files.into_iter().map(File::metadata).collect())
                .with_streaming(task.streaming)
                .with_raw_convertible(task.raw)
                .build()
                .expect("Paimon data split");
            let mut builder = table.new_read_builder();
            let mut projection = task.projection;
            if filter_deletes {
                projection.insert(
                    0,
                    DataField::new(
                        ROW_KIND_FIELD_ID,
                        ROW_KIND_FIELD_NAME.into(),
                        DataType::VarChar(VarCharType::string_type()),
                    ),
                );
            }
            builder
                .with_read_type(projection)
                .with_resources(resources.clone());
            let read = builder.new_read().expect("Paimon table reader");
            if filter_deletes {
                AuditLogRead::new(read)
                    .expect("Paimon snapshot audit reader")
                    .to_arrow(&[split])
            } else if kinds {
                read.to_arrow_with_row_kind(&[split])
            } else {
                read.to_arrow(&[split])
            }
            .expect("Paimon Arrow stream")
        });
        Self {
            stream,
            runtime,
            output,
            kinds,
            filter_deletes,
            resources,
        }
    }

    pub fn next(&mut self) -> Option<RecordBatch> {
        let batch = loop {
            let mut batch = self
                .runtime
                .block_on(self.stream.next())?
                .expect("Paimon file read");
            if self.filter_deletes {
                let kinds = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Paimon snapshot row kinds");
                let keep = BooleanArray::from_iter(
                    kinds.iter().map(|kind| matches!(kind, Some("+I" | "+U"))),
                );
                batch =
                    filter_record_batch(&batch, &keep).expect("Paimon snapshot delete selection");
            }
            if batch.num_rows() != 0 {
                break batch;
            }
        };
        let offset = usize::from(self.kinds);
        let mut columns: Vec<ArrayRef> = self.output.fields()
            [..self.output.fields().len() - offset]
            .iter()
            .enumerate()
            .map(|(index, field)| {
                streamfusion_bridge::timestamp::cast_array(
                    batch.column(index + offset),
                    field.data_type(),
                )
                .expect("Paimon output schema")
            })
            .collect();
        if self.kinds {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Paimon row kinds");
            let kinds = Int8Array::from_iter_values(values.iter().map(|kind| match kind {
                Some("+I") => 0,
                Some("-U") => 1,
                Some("+U") => 2,
                Some("-D") => 3,
                _ => panic!("Invalid Paimon row kind"),
            }));
            columns.push(Arc::new(kinds));
        }
        Some(
            RecordBatch::try_new_with_options(
                self.output.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .expect("Paimon output batch"),
        )
    }

    pub fn reserved(&self) -> usize {
        self.resources.metrics().reserved_memory_bytes
    }
}
