use super::{re, rocks_row_supported};
use crate::*;
use arrow::array::{make_array, ArrayData};
use arrow::row::{RowConverter, Rows, SortField};

/// Payload rows do not need map comparison semantics. Arrow 58's row codec can encode their
/// identical List<Struct<key,value>> layout while retaining entry order and duplicate keys.
pub(crate) struct RetainedRows {
    converter: RowConverter,
    logical: Vec<DataType>,
    physical: Vec<DataType>,
}

impl RetainedRows {
    pub(crate) fn supported(types: &[DataType]) -> bool {
        types.iter().all(|ty| match ty {
            DataType::List(field) | DataType::Map(field, _) => {
                Self::supported(std::slice::from_ref(field.data_type()))
            }
            DataType::Struct(fields) => fields
                .iter()
                .all(|field| Self::supported(std::slice::from_ref(field.data_type()))),
            _ => rocks_row_supported(std::slice::from_ref(ty)),
        })
    }

    pub(crate) fn new(types: &[DataType]) -> Result<Self, DataFusionError> {
        if !Self::supported(types) {
            return Err(DataFusionError::Plan(
                "unsupported retained row type".into(),
            ));
        }
        let physical: Vec<_> = types.iter().map(physical_type).collect();
        Ok(Self {
            converter: RowConverter::new(physical.iter().cloned().map(SortField::new).collect())
                .map_err(re)?,
            logical: types.to_vec(),
            physical,
        })
    }

    pub(crate) fn encode(&self, columns: &[ArrayRef]) -> Result<Rows, DataFusionError> {
        let columns = align_columns(columns, &self.physical)?;
        self.converter.convert_columns(&columns).map_err(re)
    }

    pub(crate) fn decode<'a>(
        &self,
        rows: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        self.decode_as(rows, &self.logical)
    }

    pub(crate) fn decode_as<'a>(
        &self,
        rows: impl IntoIterator<Item = &'a [u8]>,
        types: &[DataType],
    ) -> Result<Vec<ArrayRef>, DataFusionError> {
        let parser = self.converter.parser();
        let columns = self
            .converter
            .convert_rows(rows.into_iter().map(|bytes| parser.parse(bytes)))
            .map_err(re)?;
        align_columns(&columns, types)
    }
}

fn physical_type(ty: &DataType) -> DataType {
    let field = |field: &Arc<Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(physical_type(field.data_type())),
        )
    };
    match ty {
        DataType::Map(entries, _) | DataType::List(entries) => DataType::List(field(entries)),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        _ => ty.clone(),
    }
}

fn align_columns(
    columns: &[ArrayRef],
    types: &[DataType],
) -> Result<Vec<ArrayRef>, DataFusionError> {
    if columns.len() != types.len() {
        return Err(DataFusionError::Execution(
            "retained row arity changed".into(),
        ));
    }
    columns
        .iter()
        .zip(types)
        .map(|(array, ty)| Ok(make_array(align_data(array.to_data(), ty)?)))
        .collect()
}

fn align_data(data: ArrayData, ty: &DataType) -> Result<ArrayData, DataFusionError> {
    if data.data_type() == ty {
        return Ok(data);
    }
    let children = match ty {
        DataType::List(field) | DataType::Map(field, _) => {
            vec![align_data(data.child_data()[0].clone(), field.data_type())?]
        }
        DataType::Struct(fields) if fields.len() == data.child_data().len() => data
            .child_data()
            .iter()
            .zip(fields)
            .map(|(child, field)| align_data(child.clone(), field.data_type()))
            .collect::<Result<Vec<_>, _>>()?,
        _ => {
            return Err(DataFusionError::Execution(
                "retained row layout changed".into(),
            ))
        }
    };
    data.into_builder()
        .data_type(ty.clone())
        .child_data(children)
        .build()
        .map_err(re)
}

#[cfg(test)]
mod tests;
