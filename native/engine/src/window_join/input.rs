use super::*;

impl WindowJoiner {
    pub(super) fn logical_input(
        &self,
        batch: RecordBatch,
        left: bool,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = if left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        };
        let arity = schema.fields().len();
        if batch.num_columns() < arity {
            return Err(DataFusionError::Execution(
                "window join input lost logical fields".into(),
            ));
        }
        for (index, field) in batch.schema().fields().iter().enumerate().skip(arity) {
            if field.name() == ROW_KIND_COLUMN {
                let kinds = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("invalid window join row-kind metadata".into())
                    })?;
                if kinds.null_count() != 0 || kinds.values().iter().any(|kind| *kind != 0) {
                    return Err(DataFusionError::Execution(
                        "window joins require INSERT input".into(),
                    ));
                }
            } else if !streamfusion_bridge::is_string_cache_field(field) {
                return Err(DataFusionError::Execution(
                    "unknown window join input companion".into(),
                ));
            }
        }
        Ok(RecordBatch::try_new(
            schema.clone(),
            batch.columns()[..arity].to_vec(),
        )?)
    }
}
