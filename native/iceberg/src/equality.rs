use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::compute::{concat, filter_record_batch};
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

/// Uses the existing Iceberg decoder and DataFusion membership kernel for single-key deletes.
/// Composite keys and unverified key types retain Iceberg's normal delete predicates.
pub(crate) struct EqualitySet {
    expression: InListExpr,
    column: usize,
    has_null: bool,
    pub(crate) retained_bytes: usize,
}

impl EqualitySet {
    pub(crate) async fn load(
        io: FileIO,
        task: FileScanTask,
        schema: &arrow::datatypes::Schema,
        name: &str,
    ) -> Result<Option<Self>> {
        let reader = ArrowReaderBuilder::new(io, Runtime::new(crate::reader::runtime()))
            .with_batch_size(4096)
            .with_row_selection_enabled(false)
            .build();
        let mut batches = reader
            .read(futures::stream::iter([Ok(task)]).boxed())?
            .stream();
        let mut arrays = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            arrays.push(batch.column(0).clone());
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
        let column = schema.index_of(name).map_err(invalid)?;
        let has_null = values.null_count() > 0;
        // The expression retains both its literal list and a specialized membership set.
        // DataFusion does not expose their allocation size; charge a conservative estimate.
        let retained_bytes = values.get_array_memory_size() * 2 + values.len() * 256;
        let expression = InListExpr::try_new_from_array(
            Arc::new(Column::new(name, column)),
            values,
            false,
            schema,
        )
        .map_err(invalid)?;
        Ok(Some(Self {
            expression,
            column,
            has_null,
            retained_bytes,
        }))
    }

    pub(crate) fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let matches = self
            .expression
            .evaluate(&batch)
            .and_then(|value| value.into_array(batch.num_rows()))
            .map_err(invalid)?;
        let matches = matches
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("Equality membership must return a Boolean array"))?;
        let values = batch.column(self.column);
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
