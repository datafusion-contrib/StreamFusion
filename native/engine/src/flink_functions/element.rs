use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_list_array, exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(legacy_exception: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(Element {
        legacy_exception,
        signature: Signature::any(1, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct Element {
    legacy_exception: bool,
    signature: Signature,
}
impl ScalarUDFImpl for Element {
    fn name(&self) -> &str {
        if self.legacy_exception {
            "flink_element_legacy"
        } else {
            "flink_element"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::List(field)] => Ok(field.data_type().clone()),
            _ => exec_err!("ELEMENT requires an ARRAY"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays| element(arrays, self.legacy_exception),
            vec![],
        )(&args.args)
    }
}
fn element(args: &[ArrayRef], legacy_exception: bool) -> Result<ArrayRef> {
    let array = as_list_array(&args[0])?;
    let mut indices = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        let start = array.value_offsets()[row];
        let length = array.value_offsets()[row + 1] - start;
        indices.push(if array.is_null(row) || length == 0 {
            None
        } else if length == 1 {
            Some(start as u32)
        } else {
            return Err(DataFusionError::External(Box::new(if legacy_exception {
                streamfusion_bridge::FlinkException::java_runtime(
                    "Array has more than one element.".into(),
                )
            } else {
                streamfusion_bridge::FlinkException::table_runtime(
                    "Array has more than one element.".into(),
                )
            })));
        });
    }
    Ok(arrow::compute::take(
        array.values().as_ref(),
        &UInt32Array::from(indices),
        None,
    )?)
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, ListArray},
        datatypes::Int64Type,
    };
    use std::sync::Arc;
    #[test]
    fn only_singletons_return_a_value_and_multi_elements_keep_the_host_error() {
        let arrays: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(9)]),
                None,
                Some(vec![]),
                Some(vec![None]),
                Some(vec![Some(i64::MIN)]),
            ])
            .slice(1, 4),
        );
        assert_eq!(
            element(&[arrays], false).unwrap().as_ref(),
            &Int64Array::from(vec![None, None, None, Some(i64::MIN)])
        );
        let arrays: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            vec![None, None],
        )]));
        let error = element(&[arrays], false).unwrap_err();
        assert!(error
            .to_string()
            .contains("Array has more than one element."));
        let DataFusionError::External(cause) = error else {
            panic!("typed Flink error")
        };
        assert!(cause.is::<streamfusion_bridge::FlinkException>());
    }
}
