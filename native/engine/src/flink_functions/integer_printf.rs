use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    make_function(false)
}

pub(super) fn boolean_arguments() -> ScalarUDF {
    make_function(true)
}

fn make_function(allow_booleans: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(PrimitivePrintf {
        allow_booleans,
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PrimitivePrintf {
    allow_booleans: bool,
    signature: Signature,
}

impl ScalarUDFImpl for PrimitivePrintf {
    fn name(&self) -> &str {
        if self.allow_booleans {
            "flink_boolean_printf"
        } else {
            "flink_integer_printf"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(types)?;
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.first() != Some(&DataType::Utf8)
            || types[1..].iter().any(|t| {
                !matches!(
                    t,
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ) && !(self.allow_booleans && t == &DataType::Boolean)
            })
        {
            return exec_err!("{}: unexpected operand types {types:?}", self.name());
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        for arg in &args.args {
            if let ColumnarValue::Array(array) = arg {
                if array.len() != args.number_rows {
                    return exec_err!("Integer PRINTF inputs must have equal lengths");
                }
            }
        }
        let [ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))), values @ ..] =
            args.args.as_slice()
        else {
            return exec_err!("Integer PRINTF requires its verified non-NULL literal format");
        };
        let arrays = values
            .iter()
            .map(|arg| arg.clone().into_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let output = if self.allow_booleans {
            super::certified::boolean_printf(pattern, &arrays, args.number_rows)
        } else {
            super::certified::integer_printf(pattern, &arrays, args.number_rows)
        };
        let Some(output) = output else {
            return exec_err!("Integer PRINTF violated its verified literal-format contract");
        };
        Ok(ColumnarValue::Array(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int8Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::common::{config::ConfigOptions, ScalarValue};
    use std::sync::Arc;

    fn evaluate(args: Vec<ColumnarValue>, rows: usize) -> Result<ArrayRef> {
        function()
            .invoke_with_args(ScalarFunctionArgs {
                args,
                number_rows: rows,
                return_field: Arc::new(Field::new("v", DataType::Utf8, true)),
                arg_fields: vec![],
                config_options: Arc::new(ConfigOptions::default()),
            })?
            .into_array(rows)
    }

    #[test]
    fn signed_widths_nulls_slices_and_empty_arrays_preserve_input_ownership() {
        let input = Int8Array::from(vec![Some(-1), Some(0), None]);
        for offset in [0, 1, 3] {
            let len = 3 - offset;
            let result = evaluate(
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("%1$02x/%1$d/%1$s".into()))),
                    ColumnarValue::Array(Arc::new(input.slice(offset, len))),
                ],
                len,
            )
            .unwrap();
            let expected = StringArray::from(vec!["ff/-1/-1", "00/0/0", "null/null/null"]);
            assert_eq!(result.to_data(), expected.slice(offset, len).to_data());
        }
        assert_eq!(input, Int8Array::from(vec![Some(-1), Some(0), None]));
    }

    #[test]
    fn invalid_types_lengths_and_unverified_formats_are_rejected() {
        for types in [
            vec![],
            vec![DataType::Int32],
            vec![DataType::Utf8, DataType::Float64],
        ] {
            assert!(function().return_type(&types).is_err());
        }
        assert!(evaluate(
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%d".into()))),
                ColumnarValue::Array(Arc::new(Int8Array::from(vec![1]))),
            ],
            2
        )
        .is_err());
        assert!(evaluate(
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some("%n".into()))),],
            1
        )
        .is_err());
    }

    #[test]
    fn boolean_objects_keep_values_nullness_and_mixed_integer_widths() {
        use arrow::array::BooleanArray;
        let booleans = BooleanArray::from(vec![None, Some(false), Some(true)]);
        let integers = Int8Array::from(vec![Some(0), Some(-1), None]);
        for offset in 0..=3 {
            let rows = 3 - offset;
            let result = boolean_arguments()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                            "%1$b|%1$B|%1$s|%1$8.2S|%2$b|%2$02x".into(),
                        ))),
                        ColumnarValue::Array(Arc::new(booleans.slice(offset, rows))),
                        ColumnarValue::Array(Arc::new(integers.slice(offset, rows))),
                    ],
                    number_rows: rows,
                    return_field: Arc::new(Field::new("v", DataType::Utf8, true)),
                    arg_fields: vec![],
                    config_options: Arc::new(ConfigOptions::default()),
                })
                .unwrap()
                .into_array(rows)
                .unwrap();
            let expected = StringArray::from(vec![
                "false|FALSE|null|      NU|true|00",
                "false|FALSE|false|      FA|true|ff",
                "true|TRUE|true|      TR|false|null",
            ]);
            assert_eq!(result.to_data(), expected.slice(offset, rows).to_data());
            result.to_data().validate_full().unwrap();
        }
        assert_eq!(
            booleans,
            BooleanArray::from(vec![None, Some(false), Some(true)])
        );
        assert_eq!(integers, Int8Array::from(vec![Some(0), Some(-1), None]));
        assert!(function()
            .return_type(&[DataType::Utf8, DataType::Boolean])
            .is_err());
        assert!(boolean_arguments()
            .return_type(&[DataType::Utf8, DataType::Float64])
            .is_err());
    }
}
