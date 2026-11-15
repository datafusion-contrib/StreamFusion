use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow::compute::{concat, filter_record_batch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_physical_expr::expressions::{Column, InListExpr};
use datafusion_physical_expr::PhysicalExpr;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::{Error, ErrorKind, Result, Runtime};
use std::sync::Arc;

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::DataInvalid, error.to_string())
}

/// Iceberg decodes keys; DataFusion owns static membership, including null-safe composite keys.
pub(crate) struct EqualitySet {
    expression: InListExpr,
    keys: Vec<Arc<dyn PhysicalExpr>>,
    key_schema: SchemaRef,
    has_null: bool,
    pub(crate) retained_bytes: usize,
}

impl EqualitySet {
    pub(crate) async fn load(
        io: FileIO,
        task: FileScanTask,
        schema: &Schema,
        ids: &[i32],
    ) -> Result<Option<Self>> {
        let target = crate::schema::projected_schema(task.schema(), task.project_field_ids())?;
        let loaded_keys = ids
            .iter()
            .map(|id| crate::schema::key_expression(&target, *id).map_err(invalid))
            .collect::<Result<Vec<_>>>()?;
        let keys = ids
            .iter()
            .map(|id| crate::schema::key_expression(schema, *id).map_err(invalid))
            .collect::<Result<Vec<_>>>()?;
        let tasks = crate::scan_range::tasks(&io, task).await?;
        let reader = ArrowReaderBuilder::new(io, Runtime::new(crate::reader::runtime()))
            .with_batch_size(4096)
            .with_row_selection_enabled(false)
            .build();
        let mut batches = reader
            .read(futures::stream::iter(tasks.into_iter().map(Ok)).boxed())?
            .stream();
        let mut arrays = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            let batch = crate::schema::align(batch, target.clone()).map_err(invalid)?;
            arrays.push(key_array(&loaded_keys, &batch)?);
        }
        if arrays.is_empty() {
            return Ok(None);
        }
        let values = concat(
            &arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>(),
        )
        .map_err(invalid)?;
        let has_null = values.null_count() > 0;
        // The expression retains both its literal list and a specialized membership set.
        // DataFusion does not expose their allocation size; charge a conservative estimate.
        let retained_bytes = values.get_array_memory_size() * 2 + values.len() * 256 * ids.len();
        let key_schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            values.data_type().clone(),
            true,
        )]));
        let expression = InListExpr::try_new_from_array(
            Arc::new(Column::new("key", 0)),
            values,
            false,
            &key_schema,
        )
        .map_err(invalid)?;
        Ok(Some(Self {
            expression,
            keys,
            key_schema,
            has_null,
            retained_bytes,
        }))
    }

    pub(crate) fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let values = key_array(&self.keys, &batch)?;
        let keys =
            RecordBatch::try_new(self.key_schema.clone(), vec![values.clone()]).map_err(invalid)?;
        let matches = self
            .expression
            .evaluate(&keys)
            .and_then(|value| value.into_array(batch.num_rows()))
            .map_err(invalid)?;
        let matches = matches
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("Equality membership must return a Boolean array"))?;
        // Iceberg null equals null; SQL IN's unknown result for other rows means keep.
        let keep = BooleanArray::new(
            arrow::buffer::BooleanBuffer::collect_bool(batch.num_rows(), |row| {
                if values.is_null(row) {
                    !self.has_null
                } else {
                    matches.is_null(row) || !matches.value(row)
                }
            }),
            None,
        );
        filter_record_batch(&batch, &keep).map_err(invalid)
    }
}

fn key_array(keys: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> Result<ArrayRef> {
    let columns = keys
        .iter()
        .map(|key| {
            key.evaluate(batch)
                .and_then(|value| value.into_array(batch.num_rows()))
                .map_err(invalid)
        })
        .collect::<Result<Vec<_>>>()?;
    if columns.len() == 1 {
        return Ok(columns[0].clone());
    }
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, column)| Field::new(format!("key_{index}"), column.data_type().clone(), true))
        .collect::<Vec<_>>();
    Ok(Arc::new(StructArray::new(fields.into(), columns, None)))
}
