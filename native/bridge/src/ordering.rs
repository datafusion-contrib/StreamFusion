use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int8Array, LargeListArray, ListArray, StructArray,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

// Java Float/Double.compare canonicalize every NaN payload but distinguish signed zeros.
pub fn canonical_ordering_column(column: &ArrayRef) -> ArrayRef {
    match column.data_type() {
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            column
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f32::NAN } else { v })),
        )),
        DataType::Float64 => Arc::new(Float64Array::from_iter(
            column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f64::NAN } else { v })),
        )),
        _ => canonical_nested_row_kinds(column),
    }
}

// Flink's ROW comparator ignores RowKind. Keep the physical schema for Arrow's RowConverter,
// but compare a constant metadata child; the original payload still carries its actual kind.
fn canonical_nested_row_kinds(column: &ArrayRef) -> ArrayRef {
    match column.data_type() {
        DataType::Struct(fields) => {
            let row = column.as_any().downcast_ref::<StructArray>().unwrap();
            let kind = crate::nested_row_kind_index(fields);
            let children: Vec<_> = row
                .columns()
                .iter()
                .enumerate()
                .map(|(i, child)| {
                    if kind == Some(i) {
                        Arc::new(Int8Array::from(vec![0; child.len()])) as ArrayRef
                    } else {
                        canonical_nested_row_kinds(child)
                    }
                })
                .collect();
            if children
                .iter()
                .zip(row.columns())
                .all(|(a, b)| Arc::ptr_eq(a, b))
            {
                return column.clone();
            }
            Arc::new(StructArray::new(
                fields.clone(),
                children,
                row.nulls().cloned(),
            ))
        }
        DataType::List(field) => {
            let list = column.as_any().downcast_ref::<ListArray>().unwrap();
            let values = canonical_nested_row_kinds(list.values());
            if Arc::ptr_eq(&values, list.values()) {
                return column.clone();
            }
            Arc::new(ListArray::new(
                field.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        DataType::LargeList(field) => {
            let list = column.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = canonical_nested_row_kinds(list.values());
            if Arc::ptr_eq(&values, list.values()) {
                return column.clone();
            }
            Arc::new(LargeListArray::new(
                field.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            ))
        }
        _ => column.clone(),
    }
}
