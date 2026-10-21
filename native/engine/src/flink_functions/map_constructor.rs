use arrow::array::{ArrayRef, ListArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(arity: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(MapConstructor {
        signature: Signature::any(arity, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapConstructor {
    signature: Signature,
}
impl ScalarUDFImpl for MapConstructor {
    fn name(&self) -> &str {
        "flink_map_constructor"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.is_empty() || types.len() % 2 != 0 {
            return exec_err!("MAP requires typed key/value pairs");
        }
        if types
            .chunks_exact(2)
            .any(|p| p[0] != types[0] || p[1] != types[1])
        {
            return exec_err!("MAP requires its resolved key/value types");
        }
        Ok(super::map_arrays::map_type(
            types[0].clone(),
            types[1].clone(),
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(construct, vec![])(&args.args)
    }
}
fn construct(args: &[ArrayRef]) -> Result<ArrayRef> {
    let keys = args.iter().step_by(2).cloned().collect::<Vec<_>>();
    let values = args.iter().skip(1).step_by(2).cloned().collect::<Vec<_>>();
    let keys = datafusion::functions_nested::make_array::array_array::<i32>(
        &keys,
        keys[0].data_type().clone(),
        Field::LIST_FIELD_DEFAULT_NAME,
    )?;
    let values = datafusion::functions_nested::make_array::array_array::<i32>(
        &values,
        values[0].data_type().clone(),
        Field::LIST_FIELD_DEFAULT_NAME,
    )?;
    let keys = keys.as_any().downcast_ref::<ListArray>().unwrap();
    let values = values.as_any().downcast_ref::<ListArray>().unwrap();
    super::map_arrays::build(
        keys.values().clone(),
        values.values().clone(),
        keys.offsets().clone(),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use std::sync::Arc;
    #[test]
    fn runtime_equal_keys_remain_distinct_entries_and_null_keys_are_valid() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), Some(20)]));
        let c: ArrayRef = Arc::new(Int64Array::from(vec![Some(11), None]));
        let result = construct(&[a.clone(), b, a, c]).unwrap();
        let (entries, offsets, nulls) = super::super::map_projection::parts(&result).unwrap();
        assert_eq!(offsets.as_ref(), &[0, 2, 4]);
        assert!(nulls.is_none());
        assert_eq!(
            entries.column(0).as_ref(),
            &Int64Array::from(vec![Some(1), Some(1), None, None])
        );
        assert_eq!(
            entries.column(1).as_ref(),
            &Int64Array::from(vec![Some(10), Some(11), Some(20), None])
        );
        assert_eq!(entries.len(), 4);
    }
}
