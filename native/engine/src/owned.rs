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
