use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::fmt::Write;

use super::json_serialize::{finish, supported, write_error, JsonColumn};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonArray {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArray {
    signature: Signature,
}

impl ScalarUDFImpl for JsonArray {
    fn name(&self) -> &str {
        "flink_json_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.first() != Some(&DataType::Utf8) || !types[1..].iter().all(supported) {
            return exec_err!("JSON_ARRAY expects a NULL policy and verified scalar values");
        }
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.coerce_types(types)?;
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some((policy, fields)) = args.args.split_first() else {
            return exec_err!("JSON_ARRAY requires a NULL policy");
        };
        let absent = match policy {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(policy))) if policy == "ABSENT" => true,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(policy))) if policy == "NULL" => false,
            _ => return exec_err!("JSON_ARRAY requires a literal ABSENT or NULL policy"),
        };
        let scalar = fields
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = fields
            .iter()
            .map(|value| {
                let constant = matches!(value, ColumnarValue::Scalar(_));
                Ok((constant, value.to_array(if constant { 1 } else { rows })?))
            })
            .collect::<Result<Vec<_>>>()?;
        let values = arrays
            .iter()
            .map(|(_, array)| JsonColumn::new(array))
            .collect::<Result<Vec<_>>>()?;
        let nulls: Vec<_> = arrays
            .iter()
            .map(|(_, array)| array.logical_nulls())
            .collect();
        let mut output = StringBuilder::with_capacity(rows, rows * 32);
        for row in 0..rows {
            output.write_char('[').map_err(write_error)?;
            let mut first = true;
            for (index, (constant, _)) in arrays.iter().enumerate() {
                let value_row = if *constant { 0 } else { row };
                let is_null = nulls[index]
                    .as_ref()
                    .is_some_and(|mask| mask.is_null(value_row));
                if absent && is_null {
                    continue;
                }
                if !first {
                    output.write_char(',').map_err(write_error)?;
                }
                first = false;
                if is_null {
                    output.write_str("null").map_err(write_error)?;
                } else {
                    values[index]
                        .write(value_row, &mut output)
                        .map_err(write_error)?;
                }
            }
            output.write_char(']').map_err(write_error)?;
            super::check_string_capacity(output.values_slice().len(), 0)?;
            output.append_value("");
        }
        finish(output, scalar)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn policy(value: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.into())))
    }

    fn invoke(args: Vec<ColumnarValue>, rows: usize) -> Result<ColumnarValue> {
        function().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Utf8, false)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        })
    }

    #[test]
    fn sliced_columns_keep_null_policies_order_and_scalar_arguments() {
        let input =
            Int64Array::from(vec![Some(99), None, Some(i64::MIN), Some(i64::MAX)]).slice(1, 3);
        for (null_policy, expected) in [
            (
                "ABSENT",
                vec![
                    "[true,1.2300]",
                    "[-9223372036854775808,true,1.2300]",
                    "[9223372036854775807,true,1.2300]",
                ],
            ),
            (
                "NULL",
                vec![
                    "[null,true,null,1.2300]",
                    "[-9223372036854775808,true,null,1.2300]",
                    "[9223372036854775807,true,null,1.2300]",
                ],
            ),
        ] {
            let ColumnarValue::Array(output) = invoke(
                vec![
                    policy(null_policy),
                    ColumnarValue::Array(Arc::new(input.clone())),
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                    ColumnarValue::Scalar(ScalarValue::Null),
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(12300), 38, 4)),
                ],
                3,
            )
            .unwrap() else {
                panic!("expected array")
            };
            assert_eq!(
                datafusion::common::cast::as_string_array(&output).unwrap(),
                &StringArray::from(expected)
            );
            output.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn strings_empty_arrays_and_folded_nulls_keep_json_contract() {
        for (args, expected) in [
            (vec![policy("ABSENT")], "[]"),
            (
                vec![policy("ABSENT"), ColumnarValue::Scalar(ScalarValue::Null)],
                "[]",
            ),
            (
                vec![policy("NULL"), ColumnarValue::Scalar(ScalarValue::Null)],
                "[null]",
            ),
            (
                vec![
                    policy("NULL"),
                    policy("/\0\"\u{1f600}"),
                    ColumnarValue::Scalar(ScalarValue::Int8(Some(i8::MIN))),
                ],
                "[\"/\\u0000\\\"\u{1f600}\",-128]",
            ),
        ] {
            assert!(
                matches!(invoke(args,17).unwrap(),ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) if value==expected)
            );
        }
        let ColumnarValue::Array(output) = invoke(
            vec![
                policy("NULL"),
                ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new()))),
            ],
            0,
        )
        .unwrap() else {
            panic!("expected array")
        };
        assert_eq!(output.len(), 0);
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn unverified_types_and_nonliteral_policy_are_rejected() {
        for types in [
            vec![],
            vec![DataType::Int32],
            vec![DataType::Utf8, DataType::Float64],
        ] {
            assert!(function().coerce_types(&types).is_err());
        }
        assert!(invoke(vec![], 1).is_err());
        assert!(invoke(vec![policy("unknown")], 1).is_err());
        assert!(invoke(
            vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "NULL"
            ])))],
            1
        )
        .is_err());
    }
}
