use arrow::array::{make_array, Array, ArrayRef, ListArray, MutableArrayData, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{cast::as_list_array, exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) const MAP_LIST: &str = "streamfusion.map-list";

pub(super) fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::List(Arc::new(
        Field::new(
            "items",
            DataType::Struct(
                vec![
                    Field::new("key", key, true),
                    Field::new("value", value, true),
                ]
                .into(),
            ),
            false,
        )
        .with_metadata(HashMap::from([(MAP_LIST.into(), "1".into())])),
    ))
}

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(MapFromArrays {
        signature: Signature::any(2, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapFromArrays {
    signature: Signature,
}
impl ScalarUDFImpl for MapFromArrays {
    fn name(&self) -> &str {
        "flink_map_from_arrays"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::List(keys), DataType::List(values)] => Ok(map_type(
                keys.data_type().clone(),
                values.data_type().clone(),
            )),
            _ => exec_err!("MAP_FROM_ARRAYS requires two arrays"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(from_arrays, vec![])(&args.args)
    }
}

fn from_arrays(args: &[ArrayRef]) -> Result<ArrayRef> {
    let keys = as_list_array(&args[0])?;
    let values = as_list_array(&args[1])?;
    let validity = NullBuffer::union(keys.nulls(), values.nulls());
    for row in 0..keys.len() {
        if keys.is_valid(row)
            && values.is_valid(row)
            && keys.value_length(row) != values.value_length(row)
        {
            return Err(DataFusionError::External(Box::new(streamfusion_bridge::FlinkException::flink_runtime(
                format!("Invalid function MAP_FROM_ARRAYS call:\nThe length of the keys array {} is not equal to the length of the values array {}",keys.value_length(row),values.value_length(row))))));
        }
    }
    if keys.value_offsets() == values.value_offsets() {
        let end = *keys.value_offsets().last().unwrap() as usize;
        return build(
            keys.values().slice(0, end),
            values.values().slice(0, end),
            keys.offsets().clone(),
            validity,
        );
    }
    let kd = keys.values().to_data();
    let vd = values.values().to_data();
    let mut k = MutableArrayData::new(vec![&kd], false, kd.len());
    let mut v = MutableArrayData::new(vec![&vd], false, vd.len());
    let mut offsets = Vec::with_capacity(keys.len() + 1);
    offsets.push(0);
    for row in 0..keys.len() {
        if keys.is_valid(row) && values.is_valid(row) {
            k.extend(
                0,
                keys.value_offsets()[row] as usize,
                keys.value_offsets()[row + 1] as usize,
            );
            v.extend(
                0,
                values.value_offsets()[row] as usize,
                values.value_offsets()[row + 1] as usize,
            );
        }
        offsets.push(k.len() as i32);
    }
    build(
        make_array(k.freeze()),
        make_array(v.freeze()),
        OffsetBuffer::new(offsets.into()),
        validity,
    )
}

pub(super) fn build(
    keys: ArrayRef,
    values: ArrayRef,
    offsets: OffsetBuffer<i32>,
    validity: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let DataType::List(field) = map_type(keys.data_type().clone(), values.data_type().clone())
    else {
        unreachable!()
    };
    let DataType::Struct(fields) = field.data_type() else {
        unreachable!()
    };
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    Ok(Arc::new(ListArray::try_new(
        field,
        offsets,
        Arc::new(entries),
        validity,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::Int64Array, datatypes::Int64Type};
    #[test]
    fn aligned_sliced_arrays_share_buffers_and_keep_null_duplicate_keys() {
        let a: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(9)]),
                Some(vec![Some(1), None, Some(1)]),
                None,
                Some(vec![]),
            ])
            .slice(1, 3),
        );
        let out = from_arrays(&[a.clone(), a.clone()]).unwrap();
        let list = as_list_array(&out).unwrap();
        let entries = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            entries.column(0).to_data().buffers()[0].as_ptr(),
            as_list_array(&a).unwrap().values().to_data().buffers()[0].as_ptr()
        );
        assert_eq!(
            list.value_offsets(),
            as_list_array(&a).unwrap().value_offsets()
        );
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .null_count(),
            1
        );
        assert!(list.is_null(1));
    }
    #[test]
    fn different_offsets_are_aligned_and_null_parents_suppress_length_errors() {
        let k: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            None,
            Some(vec![None, Some(2)]),
            Some(vec![]),
        ]));
        let v: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(99)]),
            Some(vec![Some(1), Some(3)]),
            Some(vec![]),
        ]));
        let out = from_arrays(&[k, v]).unwrap();
        let out = as_list_array(&out).unwrap();
        assert_eq!(out.value_offsets(), &[0, 0, 2, 2]);
        assert!(out.is_null(0));
        let bad: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            vec![Some(1)],
        )]));
        let empty: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            vec![],
        )]));
        assert!(from_arrays(&[bad, empty])
            .unwrap_err()
            .to_string()
            .contains("keys array 1 is not equal to the length of the values array 0"));
    }
}
