use crate::*;

// Java ListVector replaces this internal UTF-16 child name at a network edge.
pub(crate) fn canonical_list_labels(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(canonical_list_labels(field.data_type())),
                    )
                })
                .collect(),
        ),
        DataType::List(field)
            if field.data_type() == &DataType::UInt16
                && !field.is_nullable()
                && matches!(field.name().as_str(), "unit" | "$data$") =>
        {
            DataType::List(Arc::new(field.as_ref().clone().with_name("unit")))
        }
        _ => data_type.clone(),
    }
}

pub(crate) fn reconcile_columns(
    columns: &[ArrayRef],
    fields: &arrow::datatypes::Fields,
) -> Result<Vec<ArrayRef>, arrow::error::ArrowError> {
    if columns.len() != fields.len() {
        return Err(arrow::error::ArrowError::SchemaError(
            "Logical input arity changed".into(),
        ));
    }
    columns
        .iter()
        .zip(fields)
        .map(|(column, field)| {
            if column.data_type() != field.data_type()
                && canonical_list_labels(column.data_type())
                    == canonical_list_labels(field.data_type())
            {
                streamfusion_bridge::timestamp::cast_array(column, field.data_type())
            } else {
                Ok(column.clone())
            }
        })
        .collect()
}

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
    let columns = reconcile_columns(&batch.columns()[..arity], schema.fields())?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carrier_labels_reconcile_without_clearing_values_or_accepting_other_schema_changes() {
        let expected = crate::flink_key::represented_row_key_fixture(2, false, 100);
        let network = crate::flink_key::row_key_fixture_with_list_label(expected.clone(), "$data$");
        assert_ne!(expected.schema(), network.schema());
        let actual = insert_only(network, &expected.schema(), "test").unwrap();
        for (expected, actual) in expected.columns().iter().zip(actual.columns()) {
            assert_eq!(expected.as_ref(), actual.as_ref());
        }
        let invalid = crate::flink_key::row_key_fixture_with_list_label(expected.clone(), "other");
        assert!(insert_only(invalid, &expected.schema(), "test").is_err());
        let mut fields = expected
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[1] = fields[1].clone().with_data_type(DataType::Float64);
        assert!(insert_only(expected.clone(), &Arc::new(Schema::new(fields)), "test").is_err());
    }
}
