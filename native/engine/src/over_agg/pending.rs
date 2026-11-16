use super::*;
use streamfusion_bridge::string_cache::is_string_cache_field;

fn companion(field: &Field) -> bool {
    field.name() == ROW_KIND_COLUMN || is_string_cache_field(field)
}

impl OverWindowAggregator {
    pub(super) fn align_pending_schema(
        &mut self,
        next: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let schema = next.schema();
        if self.buffered.iter().all(|batch| batch.schema() == schema) {
            return Ok(());
        }
        let logical = schema
            .fields()
            .iter()
            .filter(|field| !companion(field))
            .collect::<Vec<_>>();
        let mut aligned = Vec::with_capacity(self.buffered.len());
        for batch in &self.buffered {
            let previous = batch.schema();
            let payload = previous
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| !companion(field))
                .collect::<Vec<_>>();
            if payload.len() != logical.len()
                || payload.iter().zip(&logical).any(|((_, left), right)| {
                    left.metadata() != right.metadata()
                        || !crate::owned::same_logical_type(left.data_type(), right.data_type())
                })
            {
                return Err(DataFusionError::Execution(format!(
                    "OVER pending logical input schema changed: {:?} -> {:?}",
                    payload
                        .iter()
                        .map(|(_, field)| field.data_type())
                        .collect::<Vec<_>>(),
                    logical
                        .iter()
                        .map(|field| field.data_type())
                        .collect::<Vec<_>>()
                )));
            }
            let mut columns = Vec::with_capacity(schema.fields().len());
            let mut position = 0;
            for field in schema.fields() {
                if !companion(field) {
                    columns.push(align_column(
                        batch.column(payload[position].0),
                        field.data_type(),
                    )?);
                    position += 1;
                } else if let Some(column) = batch.column_by_name(field.name()) {
                    if column.data_type() != field.data_type() {
                        return Err(DataFusionError::Execution(
                            "OVER pending companion type changed".into(),
                        ));
                    }
                    columns.push(column.clone());
                } else if field.name() == ROW_KIND_COLUMN {
                    if field.data_type() != &DataType::Int8 {
                        return Err(DataFusionError::Execution(
                            "OVER RowKind must be TINYINT".into(),
                        ));
                    }
                    columns.push(Arc::new(arrow::array::Int8Array::from(vec![
                        0;
                        batch
                            .num_rows()
                    ])));
                } else {
                    columns.push(arrow::array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    ));
                }
            }
            aligned.push(RecordBatch::try_new(schema.clone(), columns)?);
        }
        self.buffered = aligned;
        Ok(())
    }
}

pub(super) fn align_column(column: &ArrayRef, ty: &DataType) -> Result<ArrayRef, DataFusionError> {
    if column.data_type() == ty {
        return Ok(column.clone());
    }
    Ok(arrow::array::make_array(crate::owned::align_nested_data(
        column.to_data(),
        ty,
    )?))
}
