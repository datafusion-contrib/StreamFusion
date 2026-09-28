use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_int32_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(crate) fn function(arity: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(BinaryElt {
        signature: Signature::any(arity, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryElt {
    signature: Signature,
}

impl ScalarUDFImpl for BinaryElt {
    fn name(&self) -> &str {
        "flink_binary_elt"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(select, vec![])(&args.args)
    }
}

fn select(args: &[ArrayRef]) -> Result<ArrayRef> {
    let Some((index, values)) = args.split_first() else {
        return exec_err!("ELT requires an index");
    };
    let indexes = as_int32_array(index)?;
    let arrays = values
        .iter()
        .map(|value| arrow::compute::cast(value, &DataType::Binary))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let arrays = arrays
        .iter()
        .map(|array| array.as_any().downcast_ref::<BinaryArray>().unwrap())
        .collect::<Vec<_>>();
    let mut output = BinaryBuilder::new();
    for (row, index) in indexes.iter().enumerate() {
        let selected = index
            .filter(|&index| index > 0)
            .and_then(|index| arrays.get(index as usize - 1));
        match selected {
            Some(array) if !array.is_null(row) => output.append_value(array.value(row)),
            _ => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn selects_one_based_indexes_and_preserves_selected_nulls() {
        let indexes: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(0),
            Some(-1),
            Some(3),
            None,
            Some(2),
        ]));
        let a: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0xff, 0][..]); 7]));
        let b: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[0, 0x80][..]),
            Some(&[0, 0x80][..]),
            None,
            None,
            None,
            None,
            None,
        ]));
        assert_eq!(
            select(&[indexes, a, b]).unwrap().as_ref(),
            &BinaryArray::from(vec![
                Some(&[0xff, 0][..]),
                Some(&[0, 0x80][..]),
                None,
                None,
                None,
                None,
                None
            ])
        );
    }
}
