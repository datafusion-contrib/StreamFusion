use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, FixedSizeBinaryArray};
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
        .map(|value| match value.data_type() {
            DataType::Binary | DataType::FixedSizeBinary(_) => Ok(Arc::clone(value)),
            _ => arrow::compute::cast(value, &DataType::Binary),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let arrays = arrays
        .iter()
        .map(|array| match array.data_type() {
            DataType::FixedSizeBinary(_) => BinaryInput::Fixed(
                array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap(),
            ),
            _ => BinaryInput::Variable(array.as_any().downcast_ref::<BinaryArray>().unwrap()),
        })
        .collect::<Vec<_>>();
    let mut output = BinaryBuilder::new();
    for (row, index) in indexes.iter().enumerate() {
        let selected = index
            .filter(|&index| index > 0)
            .and_then(|index| arrays.get(index as usize - 1));
        match selected {
            Some(array) => output.append_option(array.value(row)),
            _ => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

enum BinaryInput<'a> {
    Fixed(&'a FixedSizeBinaryArray),
    Variable(&'a BinaryArray),
}

impl BinaryInput<'_> {
    fn value(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Fixed(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Variable(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn selects_sliced_fixed_and_variable_inputs_without_padding_changes() {
        use arrow::array::FixedSizeBinaryBuilder;
        let mut fixed = FixedSizeBinaryBuilder::new(2);
        fixed.append_value([9, 9]).unwrap();
        fixed.append_value([0xff, 0]).unwrap();
        fixed.append_null();
        fixed.append_value([0, 0x80]).unwrap();
        fixed.append_value([8, 8]).unwrap();
        let fixed: ArrayRef = Arc::new(fixed.finish());
        let variable: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[9][..]),
            Some(&[][..]),
            Some(&[1, 2, 3][..]),
            None,
            Some(&[8][..]),
        ]));
        let indexes: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 1, 2, 0]));
        assert_eq!(
            select(&[indexes.slice(1, 3), fixed.slice(1, 3), variable.slice(1, 3)])
                .unwrap()
                .as_ref(),
            &BinaryArray::from(vec![Some(&[0xff, 0][..]), None, None])
        );
        let indexes: ArrayRef = Arc::new(Int32Array::from(vec![2, 2, 1]));
        assert_eq!(
            select(&[indexes, fixed.slice(1, 3), variable.slice(1, 3)])
                .unwrap()
                .as_ref(),
            &BinaryArray::from(vec![
                Some(&[][..]),
                Some(&[1, 2, 3][..]),
                Some(&[0, 0x80][..])
            ])
        );
    }

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
