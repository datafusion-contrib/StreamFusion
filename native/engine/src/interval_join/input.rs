use super::*;

pub(super) fn retained(
    batch: RecordBatch,
    schema: &SchemaRef,
    outer: bool,
) -> Result<RecordBatch, DataFusionError> {
    if !outer {
        return crate::logical_input::insert_only(batch, schema, "interval join");
    }
    let last = batch.num_columns().checked_sub(1).ok_or_else(|| {
        DataFusionError::Execution("interval join lost retained row identities".into())
    })?;
    let ids = batch
        .column(last)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("interval join has invalid retained identities".into())
        })?;
    if ids.null_count() != 0 {
        return Err(DataFusionError::Execution(
            "interval join has NULL retained identities".into(),
        ));
    }
    let data = crate::logical_input::insert_only(
        batch.project(&(0..last).collect::<Vec<_>>())?,
        schema,
        "interval join",
    )?;
    let mut columns = data.columns().to_vec();
    columns.push(batch.column(last).clone());
    Ok(RecordBatch::try_new(with_rowid_schema(schema), columns)?)
}
