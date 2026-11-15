use arrow::array::{
    Array, BooleanBufferBuilder, Float32Array, Float64Array, ListArray, MapArray, RecordBatch,
    StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;

pub(crate) fn accumulate(batch: &RecordBatch, counts: &mut HashMap<i32, u64>) {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        visit(field, column.as_ref(), None, counts);
    }
}

fn visit(
    field: &Field,
    array: &dyn Array,
    parent: Option<&NullBuffer>,
    counts: &mut HashMap<i32, u64>,
) {
    if !contains_float(field.data_type()) {
        return;
    }
    let nulls = NullBuffer::union(parent, array.nulls());
    let valid = |row| nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row));
    match field.data_type() {
        DataType::Float32 | DataType::Float64 => {
            let count = if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
                (0..values.len())
                    .filter(|&row| valid(row) && values.value(row).is_nan())
                    .count()
            } else {
                let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                (0..values.len())
                    .filter(|&row| valid(row) && values.value(row).is_nan())
                    .count()
            };
            let id = field.metadata()["PARQUET:field_id"]
                .parse()
                .expect("Iceberg field id");
            *counts.entry(id).or_default() += count as u64;
        }
        DataType::Struct(fields) => {
            let values = array.as_any().downcast_ref::<StructArray>().unwrap();
            for (field, child) in fields.iter().zip(values.columns()) {
                visit(field, child.as_ref(), nulls.as_ref(), counts);
            }
        }
        DataType::List(field) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let parent = element_nulls(list.value_offsets(), list.values().len(), nulls.as_ref());
            visit(field, list.values().as_ref(), parent.as_ref(), counts);
        }
        DataType::Map(field, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let parent = element_nulls(map.value_offsets(), map.entries().len(), nulls.as_ref());
            visit(field, map.entries(), parent.as_ref(), counts);
        }
        _ => {}
    }
}

fn contains_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 | DataType::Float64 => true,
        DataType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        DataType::List(field) | DataType::Map(field, _) => contains_float(field.data_type()),
        _ => false,
    }
}

/// Ignore child storage outside a sliced container and under logically null parents.
fn element_nulls(offsets: &[i32], len: usize, parent: Option<&NullBuffer>) -> Option<NullBuffer> {
    if parent.is_none() && offsets[0] == 0 && *offsets.last().unwrap() as usize == len {
        return None;
    }
    let mut valid = BooleanBufferBuilder::new(len);
    valid.append_n(offsets[0] as usize, false);
    for (row, range) in offsets.windows(2).enumerate() {
        valid.append_n(
            (range[1] - range[0]) as usize,
            parent.is_none_or(|p| p.is_valid(row)),
        );
    }
    valid.append_n(len - *offsets.last().unwrap() as usize, false);
    Some(NullBuffer::new(valid.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn count_only_visible_float_values_in_nested_and_sliced_containers() {
        let leaf = Arc::new(
            Field::new("f", DataType::Float64, true)
                .with_metadata(HashMap::from([("PARQUET:field_id".into(), "42".into())])),
        );
        let values = Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::NAN),
            Some(1.0),
            Some(f64::NAN),
            None,
        ]));
        let struct_values = Arc::new(StructArray::new(
            vec![leaf.clone()].into(),
            vec![values.clone()],
            Some(NullBuffer::from(vec![false, true, true, false, true])),
        )) as ArrayRef;
        let list = Arc::new(ListArray::new(
            leaf,
            OffsetBuffer::new(vec![0, 2, 3, 5].into()),
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )) as ArrayRef;
        for (array, expected) in [
            (struct_values, 1),
            (list.clone(), 3),
            (list.slice(1, 1), 0),
            (list.slice(2, 1), 1),
        ] {
            let field = Field::new("nested", array.data_type().clone(), true);
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
            let mut counts = HashMap::new();
            accumulate(&batch, &mut counts);
            assert_eq!(counts, HashMap::from([(42, expected)]));
        }
    }
}
