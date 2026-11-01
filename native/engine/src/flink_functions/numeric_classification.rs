use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn alpha() -> ScalarUDF {
    classifier("flink_numeric_is_alpha", false)
}

pub(super) fn digit() -> ScalarUDF {
    classifier("flink_numeric_is_digit", true)
}

fn classifier(name: &'static str, integral_result: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(NumericClassification {
        name,
        integral_result,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NumericClassification {
    name: &'static str,
    integral_result: bool,
    signature: Signature,
}

impl ScalarUDFImpl for NumericClassification {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)] => Ok(DataType::Boolean),
            _ => exec_err!("{} requires one exact numeric argument", self.name()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("{} requires one exact numeric argument", self.name());
        };
        let ty = input.data_type();
        self.return_type(&[ty.clone()])?;
        // Nullable primitive arguments are boxed defaults; DecimalData is not a Number.
        // Keep the argument in the expression so failures are evaluated before this call.
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
            self.integral_result && !matches!(ty, DataType::Decimal128(_, _)),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Decimal128Array, Int64Array};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn exact_numeric_arrays_scalars_nulls_and_slices_return_the_released_type_result() {
        let integers = Arc::new(Int64Array::from(vec![
            Some(99),
            None,
            Some(i64::MIN),
            Some(0),
        ])) as ArrayRef;
        let decimals = Arc::new(
            Decimal128Array::from(vec![Some(99), None, Some(-1), Some(0)])
                .with_precision_and_scale(38, 18)
                .unwrap(),
        ) as ArrayRef;
        let mut inputs = Vec::new();
        for input in [integers, decimals] {
            inputs.push(ColumnarValue::Array(input.slice(1, 3)));
            inputs.push(ColumnarValue::Array(input.slice(1, 0)));
        }
        inputs.extend([
            ColumnarValue::Scalar(ScalarValue::Int8(None)),
            ColumnarValue::Scalar(ScalarValue::Int16(Some(-1))),
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
            ColumnarValue::Scalar(ScalarValue::Decimal128(None, 38, 38)),
        ]);
        for function in [alpha(), digit()] {
            for input in inputs.clone() {
                let ty = input.data_type();
                let expected =
                    function.name().ends_with("digit") && !matches!(ty, DataType::Decimal128(_, _));
                let rows = match &input {
                    ColumnarValue::Array(array) => array.len(),
                    _ => 3,
                };
                let result = function
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![input],
                        arg_fields: vec![Arc::new(Field::new("v", ty, true))],
                        number_rows: rows,
                        return_field: Arc::new(Field::new("result", DataType::Boolean, false)),
                        config_options: Arc::new(Default::default()),
                    })
                    .unwrap();
                assert!(
                    matches!(result, ColumnarValue::Scalar(ScalarValue::Boolean(Some(actual))) if actual == expected)
                );
                let array = result.into_array(rows).unwrap();
                assert_eq!(array.len(), rows);
                assert_eq!(array.null_count(), 0);
            }
        }
    }

    #[test]
    fn unqualified_signatures_are_rejected_without_coercion() {
        for function in [alpha(), digit()] {
            for types in [
                vec![],
                vec![DataType::Float64],
                vec![DataType::Utf8],
                vec![DataType::Boolean],
                vec![DataType::Int32, DataType::Int32],
            ] {
                assert!(function.return_type(&types).is_err());
            }
        }
    }
}
