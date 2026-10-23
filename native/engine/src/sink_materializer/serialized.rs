use crate::*;

pub(super) fn batch(batch: &RecordBatch) -> Option<RecordBatch> {
    let mut columns = None;
    for (i, (field, array)) in batch
        .schema_ref()
        .fields()
        .iter()
        .zip(batch.columns())
        .enumerate()
    {
        if let Some(array) = column(field, array, false) {
            columns.get_or_insert_with(|| batch.columns().to_vec())[i] = array;
        }
    }
    columns.map(|columns| {
        RecordBatch::try_new(batch.schema(), columns).expect("serialized sink values")
    })
}

fn marked(field: &Field, name: &str) -> bool {
    field.metadata().get(name).is_some_and(|v| v == "1")
}

fn uncanonical_nan(array: &ArrayRef) -> bool {
    match array.data_type() {
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .flatten()
            .any(|v| v.is_nan() && v.to_bits() != crate::flink_float::canonical_f32(v).to_bits()),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .iter()
            .flatten()
            .any(|v| v.is_nan() && v.to_bits() != crate::flink_float::canonical_f64(v).to_bits()),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => array
            .to_data()
            .child_data()
            .iter()
            .any(|child| uncanonical_nan(&arrow::array::make_array(child.clone()))),
        _ => false,
    }
}

fn column(field: &Field, array: &ArrayRef, element: bool) -> Option<ArrayRef> {
    if !element && matches!(array.data_type(), DataType::Float32 | DataType::Float64)
        || !uncanonical_nan(array)
    {
        return None;
    }
    let mut changed = false;
    let values = (0..array.len())
        .map(|i| {
            let old = ScalarValue::try_from_array(array, i).expect("sink scalar");
            value(field, &old, element)
                .map(|new| {
                    changed = true;
                    new
                })
                .unwrap_or(old)
        })
        .collect::<Vec<_>>();
    changed.then(|| ScalarValue::iter_to_array(values).expect("normalized sink column"))
}

fn entries(array: &StructArray) -> Option<StructArray> {
    let mut changed = false;
    let columns = array
        .fields()
        .iter()
        .zip(array.columns())
        .map(|(field, values)| {
            column(field, values, true)
                .map(|new| {
                    changed = true;
                    new
                })
                .unwrap_or_else(|| values.clone())
        })
        .collect();
    changed.then(|| StructArray::new(array.fields().clone(), columns, array.nulls().cloned()))
}

fn value(field: &Field, old: &ScalarValue, element: bool) -> Option<ScalarValue> {
    if old.is_null() {
        return None;
    }
    if element {
        match old {
            ScalarValue::Float32(Some(v))
                if v.is_nan() && v.to_bits() != crate::flink_float::canonical_f32(*v).to_bits() =>
            {
                return Some(ScalarValue::Float32(Some(
                    crate::flink_float::canonical_f32(*v),
                )))
            }
            ScalarValue::Float64(Some(v))
                if v.is_nan() && v.to_bits() != crate::flink_float::canonical_f64(*v).to_bits() =>
            {
                return Some(ScalarValue::Float64(Some(
                    crate::flink_float::canonical_f64(*v),
                )))
            }
            _ => {}
        }
    }
    match old {
        ScalarValue::Struct(array) => {
            let fields = array.fields();
            let carrier = fields.len() == 2 && marked(&fields[1], "streamfusion.array-element");
            if carrier {
                let flags = array
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                if !flags.is_null(0) && flags.value(0) != 0 {
                    return None;
                }
            } else if let Some(kind) = nested_row_kind_index(fields) {
                let flags = array
                    .column(kind)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                if !flags.is_null(0) && flags.value(0) & (4 | 16) != 0 {
                    return None;
                }
            }
            let mut changed = false;
            let columns = fields
                .iter()
                .zip(array.columns())
                .enumerate()
                .map(|(i, (f, a))| {
                    column(f, a, carrier && i == 0)
                        .map(|a| {
                            changed = true;
                            a
                        })
                        .unwrap_or_else(|| a.clone())
                })
                .collect();
            changed.then(|| {
                ScalarValue::Struct(Arc::new(StructArray::new(
                    fields.clone(),
                    columns,
                    array.nulls().cloned(),
                )))
            })
        }
        ScalarValue::List(array) => {
            let DataType::List(child) = field.data_type() else {
                unreachable!()
            };
            let values = array.value(0);
            let values = if marked(field, "streamfusion.map-list")
                || marked(child, "streamfusion.map-list")
            {
                entries(values.as_any().downcast_ref::<StructArray>().unwrap())
                    .map(|v| Arc::new(v) as ArrayRef)
            } else {
                column(child, &values, true)
            }?;
            Some(ScalarValue::List(Arc::new(ListArray::new(
                child.clone(),
                arrow::buffer::OffsetBuffer::new(vec![0, values.len() as i32].into()),
                values,
                None,
            ))))
        }
        ScalarValue::Map(array) => {
            let DataType::Map(child, ordered) = field.data_type() else {
                unreachable!()
            };
            let values = entries(&array.value(0))?;
            let offsets = arrow::buffer::OffsetBuffer::new(vec![0, values.len() as i32].into());
            Some(ScalarValue::Map(Arc::new(MapArray::new(
                child.clone(),
                offsets,
                values,
                None,
                *ordered,
            ))))
        }
        _ => None,
    }
}
