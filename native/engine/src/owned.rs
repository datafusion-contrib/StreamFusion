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
