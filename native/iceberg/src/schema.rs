use arrow::array::{
    new_null_array, Array, ArrayRef, ListArray, MapArray, RecordBatch, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use iceberg::spec::{ListType, MapType, NestedFieldRef, Schema as IcebergSchema, StructType, Type};
use iceberg::Result;
use std::sync::Arc;

fn field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .and_then(|id| id.parse().ok())
}

pub(crate) fn root_id(schema: &IcebergSchema, id: i32) -> Option<i32> {
    fn contains(field: &NestedFieldRef, id: i32) -> bool {
        field.id == id
            || match field.field_type.as_ref() {
                Type::Struct(fields) => fields.fields().iter().any(|field| contains(field, id)),
                _ => false,
            }
    }
    schema
        .as_struct()
        .fields()
        .iter()
        .find(|field| contains(field, id))
        .map(|field| field.id)
}

pub(crate) fn projected_schema(schema: &IcebergSchema, ids: &[i32]) -> Result<SchemaRef> {
    let arrow: Schema = schema.try_into()?;
    let fields = ids
        .iter()
        .filter_map(|id| {
            arrow
                .fields()
                .iter()
                .find(|field| field_id(field) == Some(*id))
                .cloned()
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn key_expression(
    schema: &Schema,
    id: i32,
) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
    fn path(fields: &arrow::datatypes::Fields, id: i32) -> Option<Vec<usize>> {
        for (index, field) in fields.iter().enumerate() {
            if field_id(field) == Some(id) {
                return Some(vec![index]);
            }
            if let DataType::Struct(fields) = field.data_type() {
                if let Some(mut child) = path(fields, id) {
                    child.insert(0, index);
                    return Some(child);
                }
            }
        }
        None
    }
    let path = path(schema.fields(), id).ok_or_else(|| {
        datafusion_common::DataFusionError::Plan(format!("Missing equality field ID {id}"))
    })?;
    let index = path[0];
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(schema.field(index).name(), index));
    if path.len() == 1 {
        return Ok(column);
    }
    let mut args = vec![column];
    let mut field = schema.field(index);
    for &index in &path[1..] {
        let DataType::Struct(fields) = field.data_type() else {
            unreachable!()
        };
        field = &fields[index];
        args.push(Arc::new(Literal::new(
            datafusion_common::ScalarValue::Utf8(Some(field.name().clone())),
        )));
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::core::get_field(),
        args,
        schema,
        Default::default(),
    )?))
}

/// RC3 evolves top-level columns. Retain the stored nested shape for its decoder, then evolve
/// nested fields by ID on the resulting Arrow arrays before exposing the logical batch.
pub(crate) fn read_schema(
    target: &Arc<IcebergSchema>,
    physical: &Schema,
) -> Result<Arc<IcebergSchema>> {
    let mut fields = Vec::new();
    for field in target.as_struct().fields() {
        let mut field = field.as_ref().clone();
        if !field.field_type.is_primitive() {
            if let Some(stored) = physical
                .fields()
                .iter()
                .find(|stored| field_id(stored) == Some(field.id))
            {
                let stored = iceberg::arrow::arrow_type_to_type(stored.data_type())?;
                field.field_type = Box::new(read_type(&stored, target));
            }
        }
        fields.push(Arc::new(field));
    }
    Ok(Arc::new(
        IcebergSchema::builder().with_fields(fields).build()?,
    ))
}

fn read_field(stored: &NestedFieldRef, target: &IcebergSchema) -> NestedFieldRef {
    let mut field = stored.as_ref().clone();
    field.field_type = Box::new(match field.field_type.as_ref() {
        Type::Primitive(_) => target.field_by_id(field.id).map_or_else(
            || *field.field_type.clone(),
            |logical| *logical.field_type.clone(),
        ),
        nested => read_type(nested, target),
    });
    Arc::new(field)
}

fn read_type(stored: &Type, target: &IcebergSchema) -> Type {
    match stored {
        Type::Struct(fields) => Type::Struct(StructType::new(
            fields
                .fields()
                .iter()
                .map(|field| read_field(field, target))
                .collect(),
        )),
        Type::List(list) => Type::List(ListType {
            element_field: read_field(&list.element_field, target),
        }),
        Type::Map(map) => Type::Map(MapType {
            key_field: read_field(&map.key_field, target),
            value_field: read_field(&map.value_field, target),
        }),
        primitive => primitive.clone(),
    }
}

pub(crate) fn align(
    batch: RecordBatch,
    target: SchemaRef,
) -> std::result::Result<RecordBatch, ArrowError> {
    let source = batch.schema();
    let columns = target
        .fields()
        .iter()
        .map(|field| {
            let index = source
                .fields()
                .iter()
                .position(|candidate| same_field(candidate, field));
            match index {
                Some(index) => align_array(batch.column(index), field),
                None => missing(field, batch.num_rows(), false),
            }
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        target,
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

fn same_field(left: &Field, right: &Field) -> bool {
    match (field_id(left), field_id(right)) {
        (Some(left), Some(right)) => left == right,
        _ => left.name() == right.name(),
    }
}

fn missing(
    field: &Field,
    len: usize,
    null_parent: bool,
) -> std::result::Result<ArrayRef, ArrowError> {
    if !field.is_nullable() && !null_parent && len != 0 {
        return Err(ArrowError::SchemaError(format!(
            "Missing required Iceberg field {}",
            field.name()
        )));
    }
    Ok(new_null_array(field.data_type(), len))
}

fn align_array(array: &ArrayRef, target: &Field) -> std::result::Result<ArrayRef, ArrowError> {
    let source = array.data_type();
    let data_type = target.data_type();
    if source == data_type && !data_type.is_nested() {
        return Ok(array.clone());
    }
    if streamfusion_bridge::timestamp::is_timestamp(source)
        && streamfusion_bridge::timestamp::is_timestamp(data_type)
    {
        return streamfusion_bridge::timestamp::cast_array(array, data_type);
    }
    match (source, data_type) {
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let values = array.as_any().downcast_ref::<StructArray>().unwrap();
            let columns = fields
                .iter()
                .map(|field| {
                    match values
                        .fields()
                        .iter()
                        .position(|stored| same_field(stored, field))
                    {
                        Some(index) => {
                            let child = values.column(index);
                            let child = if values.null_count() == 0 {
                                child.clone()
                            } else {
                                arrow::array::make_array(
                                    child
                                        .to_data()
                                        .into_builder()
                                        .nulls(arrow::buffer::NullBuffer::union(
                                            values.nulls(),
                                            child.nulls(),
                                        ))
                                        .build()?,
                                )
                            };
                            align_array(&child, field)
                        }
                        None => missing(field, values.len(), values.null_count() == values.len()),
                    }
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                values.nulls().cloned(),
                values.len(),
            )?))
        }
        (DataType::List(_), DataType::List(field)) => {
            let values = array.as_any().downcast_ref::<ListArray>().unwrap();
            Ok(Arc::new(ListArray::try_new(
                field.clone(),
                values.offsets().clone(),
                align_array(values.values(), field)?,
                values.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, _), DataType::Map(field, sorted)) => {
            let values = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = align_array(&(Arc::new(values.entries().clone()) as ArrayRef), field)?;
            Ok(Arc::new(MapArray::try_new(
                field.clone(),
                values.offsets().clone(),
                entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .clone(),
                values.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => streamfusion_bridge::timestamp::cast_array(array, data_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::buffer::NullBuffer;
    use std::collections::HashMap;

    #[test]
    fn nested_keys_observe_parent_nulls_without_copying_child_values() {
        let child = Arc::new(
            Field::new("n", DataType::Int32, false)
                .with_metadata(HashMap::from([("PARQUET:field_id".into(), "3".into())])),
        );
        let raw = Arc::new(Int32Array::from(vec![10, 99, 20, 0]));
        let structure = Arc::new(StructArray::new(
            vec![child].into(),
            vec![raw.clone()],
            Some(NullBuffer::from(vec![true, false, true, true])),
        ));
        let field = Field::new("profile", structure.data_type().clone(), true)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "2".into())]));
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![structure]).unwrap();
        let batch = align(batch, schema).unwrap();
        let key = key_expression(&batch.schema(), 3)
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(4)
            .unwrap();
        let values = key.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(10), None, Some(20), Some(0)]
        );
        assert_eq!(values.values().as_ptr(), raw.values().as_ptr());
    }
}
