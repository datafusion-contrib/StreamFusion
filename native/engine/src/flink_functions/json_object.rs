use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::fmt::Write;

use super::json_serialize::{finish, supported, write_error, JsonColumn};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonObject {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObject {
    signature: Signature,
}

impl ScalarUDFImpl for JsonObject {
    fn name(&self) -> &str {
        "flink_json_object"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.is_empty()
            || types.len() % 2 != 1
            || types[0] != DataType::Utf8
            || types[1..]
                .chunks_exact(2)
                .any(|pair| pair[0] != DataType::Utf8 || !supported(&pair[1]))
        {
            return exec_err!(
                "JSON_OBJECT expects a NULL policy and character keys with scalar values"
            );
        }
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.coerce_types(types)?;
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some((policy, fields)) = args.args.split_first() else {
            return exec_err!("JSON_OBJECT requires a NULL policy");
        };
        let absent = match literal(policy)? {
            "ABSENT" => true,
            "NULL" => false,
            _ => return exec_err!("Unknown JSON_OBJECT NULL policy"),
        };
        if fields.len() % 2 != 0 {
            return exec_err!("JSON_OBJECT requires key/value pairs");
        }
        let scalar = fields
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let mut names = Vec::with_capacity(fields.len() / 2);
        let mut arrays = Vec::with_capacity(names.capacity());
        for pair in fields.chunks_exact(2) {
            names.push(literal(&pair[0])?);
            let constant = matches!(&pair[1], ColumnarValue::Scalar(_));
            arrays.push((constant, pair[1].to_array(if constant { 1 } else { rows })?));
        }
        let values: Vec<_> = arrays
            .iter()
            .map(|(_, array)| JsonColumn::new(array))
            .collect::<Result<_>>()?;
        // Jackson orders keys with String.compareTo (UTF-16), and the last inserted value wins.
        // Stable sorting retains insertion order within each duplicate-key group.
        let mut indices: Vec<usize> = (0..names.len()).collect();
        indices.sort_by(|&a, &b| names[a].encode_utf16().cmp(names[b].encode_utf16()));
        let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
        for index in indices {
            if let Some((_, previous)) = groups.last_mut() {
                if names[previous[0]] == names[index] {
                    previous.push(index);
                    continue;
                }
            }
            let mut key = String::new();
            crate::json_string::write_json_string(&mut key, names[index]).map_err(write_error)?;
            key.push(':');
            groups.push((key, vec![index]));
        }
        let mut output = StringBuilder::with_capacity(rows, rows * 32);
        for row in 0..rows {
            output.write_char('{').map_err(write_error)?;
            let mut first = true;
            for (key, indices) in &groups {
                let selected = indices.iter().rev().find(|&&index| {
                    let (constant, array) = &arrays[index];
                    !absent || !array.is_null(if *constant { 0 } else { row })
                });
                let Some(&index) = selected else { continue };
                if !first {
                    output.write_char(',').map_err(write_error)?;
                }
                first = false;
                output.write_str(key).map_err(write_error)?;
                let (constant, array) = &arrays[index];
                let value_row = if *constant { 0 } else { row };
                if array.is_null(value_row) {
                    output.write_str("null").map_err(write_error)?;
                } else {
                    values[index]
                        .write(value_row, &mut output)
                        .map_err(write_error)?;
                }
            }
            output.write_char('}').map_err(write_error)?;
            super::check_string_capacity(output.values_slice().len(), 0)?;
            output.append_value("");
        }
        finish(output, scalar)
    }
}

fn literal(value: &ColumnarValue) -> Result<&str> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => Ok(value),
        _ => exec_err!("JSON_OBJECT requires non-null literal keys and NULL policy"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn string(value: &str) -> ColumnarValue {
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
    fn duplicates_null_policies_utf16_key_order_and_sliced_values() {
        let values = StringArray::from(vec![Some("skip"), None, Some("last")]).slice(1, 2);
        for (policy, first) in [("NULL", "null"), ("ABSENT", "\"first\"")] {
            let ColumnarValue::Array(output) = invoke(
                vec![
                    string(policy),
                    string("\u{fffd}"),
                    string("bmp"),
                    string("\u{1f600}"),
                    string("astral"),
                    string("k"),
                    string("first"),
                    string("k"),
                    ColumnarValue::Array(Arc::new(values.clone())),
                ],
                2,
            )
            .unwrap() else {
                panic!("expected array")
            };
            let output = datafusion::common::cast::as_string_array(&output).unwrap();
            assert_eq!(
                output.value(0),
                format!("{{\"k\":{first},\"\u{1f600}\":\"astral\",\"\u{fffd}\":\"bmp\"}}")
            );
            assert_eq!(
                output.value(1),
                "{\"k\":\"last\",\"\u{1f600}\":\"astral\",\"\u{fffd}\":\"bmp\"}"
            );
            output.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn empty_objects_are_non_null_scalars_and_bad_arguments_fail() {
        assert!(matches!(invoke(vec![string("NULL")], 5).unwrap(),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) if value == "{}"));
        assert!(invoke(vec![], 1).is_err());
        assert!(invoke(vec![string("NULL"), string("key")], 1).is_err());
        assert!(invoke(vec![string("INVALID")], 1).is_err());
        assert!(function()
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Float64])
            .is_err());
    }
}
