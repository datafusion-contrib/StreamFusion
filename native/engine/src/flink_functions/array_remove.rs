use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayRemove {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayRemove {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayRemove {
    fn name(&self) -> &str {
        "flink_array_remove"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [list @ DataType::List(field), needle] if field.data_type() == needle => {
                Ok(list.clone())
            }
            _ => exec_err!("ARRAY_REMOVE requires an ARRAY and a matching element"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(remove, vec![])(&args.args)
    }
}

fn remove(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = as_list_array(&args[0])?;
    // Arrow's null ordering gives NULL = NULL here, as Flink's removal predicate requires.
    let equal = arrow_ord::ord::make_comparator(
        array.values().as_ref(),
        args[1].as_ref(),
        Default::default(),
    )?;
    let mut indices = Vec::with_capacity(array.values().len());
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(0);
    for row in 0..array.len() {
        if array.is_valid(row) {
            for index in
                array.value_offsets()[row] as usize..array.value_offsets()[row + 1] as usize
            {
                if !equal(index, row).is_eq() {
                    indices.push(index as u32);
                }
            }
        }
        offsets.push(indices.len() as i32);
    }
    let DataType::List(field) = array.data_type() else {
        unreachable!()
    };
    Ok(Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        arrow::compute::take(array.values().as_ref(), &UInt32Array::from(indices), None)?,
        array.nulls().cloned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::Int64Array, datatypes::Int64Type};
    #[test]
    fn removes_all_matches_including_null_needles_but_not_null_containers() {
        let input: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(99)]),
                Some(vec![None, Some(1), None, Some(1), Some(2)]),
                Some(vec![None, Some(1), None, Some(1), Some(2)]),
                None,
                Some(vec![]),
            ])
            .slice(1, 4),
        );
        let needles: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1), None, None]));
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), Some(1), Some(2)]),
            Some(vec![None, None, Some(2)]),
            None,
            Some(vec![]),
        ]);
        assert_eq!(remove(&[input, needles]).unwrap().as_ref(), &expected);
    }
}
