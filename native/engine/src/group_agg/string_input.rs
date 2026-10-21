use super::*;
use std::borrow::Cow;

/// Primitive STRING consumers use Flink's materialized UTF-8 bytes. Nested payloads retain caches.
pub(super) fn canonical(batch: &RecordBatch) -> Result<Cow<'_, RecordBatch>, DataFusionError> {
    if !batch
        .columns()
        .iter()
        .any(|c| crate::flink_key::represented_string(c.data_type()))
    {
        return Ok(Cow::Borrowed(batch));
    }
    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();
    for (index, source) in batch.columns().iter().enumerate() {
        if !crate::flink_key::represented_string(source.data_type()) {
            continue;
        }
        let source = source
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let child = source.column(0);
        columns[index] = if source.nulls().is_some() {
            let nulls = arrow::buffer::NullBuffer::union(source.nulls(), child.nulls());
            arrow::array::make_array(child.to_data().into_builder().nulls(nulls).build()?)
        } else {
            child.clone()
        };
        fields[index] = Arc::new(
            fields[index]
                .as_ref()
                .clone()
                .with_data_type(DataType::Utf8),
        );
    }
    Ok(Cow::Owned(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )?))
}
