use crate::*;

pub(crate) fn own_array_data(
    data: arrow::array::ArrayData,
    copy_buffers: bool,
) -> Result<arrow::array::ArrayData, arrow::error::ArrowError> {
    let copy_children = copy_buffers || matches!(data.data_type(), DataType::Dictionary(_, _));
    let children = data
        .child_data()
        .iter()
        .map(|child| own_array_data(child.clone(), copy_children))
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = data.clone().into_builder().child_data(children);
    if copy_buffers {
        builder = builder
            .buffers(
                data.buffers()
                    .iter()
                    .map(|buffer| arrow::buffer::Buffer::from_slice_ref(buffer.as_slice()))
                    .collect(),
            )
            .nulls(
                data.nulls()
                    .map(|nulls| nulls.iter().collect::<arrow::buffer::NullBuffer>()),
            );
    }
    builder.build()
}

/// Reconciles nested schema metadata over shared buffers after the caller verifies
/// logical compatibility. Retained row codecs also use this for Map's List storage.
pub(crate) fn align_nested_data(
    data: arrow::array::ArrayData,
    ty: &DataType,
) -> Result<arrow::array::ArrayData, DataFusionError> {
    if data.data_type() == ty {
        return Ok(data);
    }
    let children = match ty {
        DataType::List(field) | DataType::Map(field, _) if data.child_data().len() == 1 => {
            vec![align_nested_data(
                data.child_data()[0].clone(),
                field.data_type(),
            )?]
        }
        DataType::Struct(fields) if fields.len() == data.child_data().len() => data
            .child_data()
            .iter()
            .zip(fields)
            .map(|(child, field)| align_nested_data(child.clone(), field.data_type()))
            .collect::<Result<Vec<_>, _>>()?,
        _ => {
            return Err(DataFusionError::Execution(format!(
                "nested array layout changed: {} -> {ty}",
                data.data_type()
            )))
        }
    };
    data.into_builder()
        .data_type(ty.clone())
        .child_data(children)
        .build()
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

// Arrow Java and retained row reconstruction use different names for a List's
// physical child (element, $data$, items, entries). Those names have no SQL
// meaning. ROW field names, nullability, dictionaries and extension metadata do.
pub(crate) fn same_logical_field(left: &Field, right: &Field, element: bool) -> bool {
    let mut expected = left.clone().with_data_type(right.data_type().clone());
    if element {
        expected = expected.with_name(right.name());
    }
    expected == *right && same_logical_type(left.data_type(), right.data_type())
}

pub(crate) fn same_logical_type(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }
    match (left, right) {
        (DataType::List(left), DataType::List(right)) => same_logical_field(left, right, true),
        (DataType::Map(left, left_sorted), DataType::Map(right, right_sorted)) => {
            left_sorted == right_sorted && same_logical_field(left, right, true)
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| same_logical_field(left, right, false))
        }
        _ => false,
    }
}

/// Keeps the logical payload schema stable across Arrow's list-child aliases.
/// Extra transport columns, including RowKind, retain their original schema.
pub(crate) fn align_batch_prefix(
    batch: &RecordBatch,
    expected: &arrow::datatypes::Fields,
) -> Result<RecordBatch, DataFusionError> {
    if batch.num_columns() < expected.len() {
        return Err(DataFusionError::Execution(
            "retained input arity changed".into(),
        ));
    }
    let mut fields = batch.schema().fields().to_vec();
    let mut columns = batch.columns().to_vec();
    for (index, field) in expected.iter().enumerate() {
        if !same_logical_field(&fields[index], field, false) {
            return Err(DataFusionError::Execution(format!(
                "retained input type changed: {:?} -> {:?}",
                fields[index], field
            )));
        }
        columns[index] = arrow::array::make_array(align_nested_data(
            columns[index].to_data(),
            field.data_type(),
        )?);
        fields[index] = field.clone();
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )?)
}
