use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef, ListArray, MutableArrayData};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(arity: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayConcat {
        signature: Signature::any(arity, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayConcat {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayConcat {
    fn name(&self) -> &str {
        "flink_array_concat"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if let Some(DataType::List(field)) = types.first() {
            if types
                .iter()
                .all(|t| matches!(t,DataType::List(f) if f.data_type()==field.data_type()))
            {
                return Ok(DataType::new_list(field.data_type().clone(), true));
            }
        }
        exec_err!("ARRAY_CONCAT requires matching ARRAY types")
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(concat, vec![])(&args.args)
    }
}

fn concat(args: &[ArrayRef]) -> Result<ArrayRef> {
    let arrays = args
        .iter()
        .map(|a| as_list_array(a.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    if arrays.len() == 1 {
        return Ok(args[0].clone());
    }
    let first = arrays[0];
    let data = arrays
        .iter()
        .map(|a| a.values().to_data())
        .collect::<Vec<_>>();
    let mut values = MutableArrayData::new(
        data.iter().collect(),
        false,
        data.iter().map(|d| d.len()).sum(),
    );
    let mut offsets = Vec::with_capacity(first.len() + 1);
    let mut valid = Vec::with_capacity(first.len());
    offsets.push(0);
    for row in 0..first.len() {
        let present = arrays.iter().all(|a| a.is_valid(row));
        valid.push(present);
        if present {
            for (source, a) in arrays.iter().enumerate() {
                values.extend(
                    source,
                    a.value_offsets()[row] as usize,
                    a.value_offsets()[row + 1] as usize,
                );
            }
        }
        offsets.push(i32::try_from(values.len()).map_err(|_| {
            datafusion::common::DataFusionError::Execution("ARRAY exceeds Arrow offsets".into())
        })?);
    }
    Ok(Arc::new(ListArray::new(
        Arc::new(arrow::datatypes::Field::new_list_field(
            first.value_type(),
            true,
        )),
        OffsetBuffer::new(offsets.into()),
        make_array(values.freeze()),
        Some(NullBuffer::from(valid)),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Int64Type;
    #[test]
    fn any_null_operand_nulls_the_result_and_variadic_order_is_stable() {
        let a: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(99)]),
                Some(vec![Some(3), None]),
                None,
                Some(vec![]),
                Some(vec![Some(1)]),
            ])
            .slice(1, 4),
        );
        let b: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(2)]),
            Some(vec![Some(2)]),
            Some(vec![]),
            None,
        ]));
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(3), None, Some(2), Some(3), None]),
            None,
            Some(vec![]),
            None,
        ]);
        assert_eq!(concat(&[a.clone(), b, a]).unwrap().as_ref(), &expected);
        let empty: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(Vec::<
            Option<Vec<Option<i64>>>,
        >::new(
        )));
        assert_eq!(concat(&[empty.clone(), empty]).unwrap().len(), 0);
    }
}
