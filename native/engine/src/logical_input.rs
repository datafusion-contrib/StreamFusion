use crate::*;

pub(crate) fn insert_only(
    batch: RecordBatch,
    schema: &SchemaRef,
    operator: &str,
) -> Result<RecordBatch, DataFusionError> {
    let invalid = |reason| DataFusionError::Execution(format!("{operator} {reason}"));
    let arity = schema.fields().len();
    if batch.num_columns() < arity {
        return Err(invalid("input lost logical fields"));
    }
    for (index, field) in batch.schema().fields().iter().enumerate().skip(arity) {
        if field.name() == ROW_KIND_COLUMN {
            let kinds = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| invalid("has invalid row-kind metadata"))?;
            if kinds.null_count() != 0 || kinds.values().iter().any(|kind| *kind != 0) {
                return Err(invalid("requires INSERT input"));
            }
        } else if !streamfusion_bridge::is_string_cache_field(field) {
            return Err(invalid("has an unknown input companion"));
        }
    }
    Ok(RecordBatch::try_new(
        schema.clone(),
        batch.columns()[..arity].to_vec(),
    )?)
}
