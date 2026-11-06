use super::json_path::{with_reader, write_container, write_scalar, Path, Value};
use super::json_serialize::{finish, write_error};
use super::json_value::{escaped_document, literal};
use arrow::array::{Array, StringBuilder, StructArray};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::fmt::Write;

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonQuery {
        signature: Signature::any(6, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonQuery {
    signature: Signature,
}

impl ScalarUDFImpl for JsonQuery {
    fn name(&self) -> &str {
        "flink_json_query"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input, path, wrapper, empty, error, unicode] = args.args.as_slice() else {
            return exec_err!("JSON_QUERY expects a document, path, wrapper and NULL policies");
        };
        let path = Path::parse(
            literal(path)?.unwrap_or(""),
            literal(unicode)?.unwrap_or(""),
        )
        .filter(Path::definite)
        .ok_or_else(|| datafusion::common::exec_datafusion_err!("Unverified JSON_QUERY path"))?;
        let wrapper = literal(wrapper)?.unwrap_or("");
        if !matches!(
            wrapper,
            "WITHOUT_ARRAY" | "CONDITIONAL_ARRAY" | "UNCONDITIONAL_ARRAY"
        ) {
            return exec_err!("Unverified JSON_QUERY wrapper");
        }
        let empty = policy(empty)?;
        let error = policy(error)?;
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let represented = input.as_any().downcast_ref::<StructArray>();
        let caches = represented
            .map(|input| streamfusion_bridge::JavaStringCacheColumn::new(input.column(1)));
        let text = as_string_array(represented.map(|input| input.column(0)).unwrap_or(&input))?;
        let mut output = StringBuilder::with_capacity(text.len(), text.values().len());
        with_reader(|reader| {
            let legacy = reader.legacy_semantics();
            for (row, document) in text.iter().enumerate() {
                if represented.is_some_and(|input| input.is_null(row)) || document.is_none() {
                    output.append_null();
                    continue;
                }
                let document = document.unwrap();
                let original = caches
                    .as_ref()
                    .and_then(|cache| cache.value(row))
                    .filter(|units| !units.iter().copied().eq(document.encode_utf16()))
                    .map(escaped_document);
                if original.is_none() {
                    if let Some(result) =
                        reader.write_query_container(&path, document, wrapper, &mut output)
                    {
                        result.map_err(write_error)?;
                        super::check_string_capacity(output.values_slice().len(), 0)?;
                        output.append_value("");
                        continue;
                    }
                }
                let value = match original.as_ref() {
                    Some(original) => reader.read_query(&path, &original.text, &original.escapes),
                    None => reader.read_query(&path, document, &[]),
                };
                match value {
                    Err(()) => output.append_option(error),
                    Ok(Value::Missing | Value::Null) => output.append_option(empty),
                    Ok(Value::CapturedContainer(raw)) => {
                        let wrap = wrapper == "UNCONDITIONAL_ARRAY"
                            || wrapper == "CONDITIONAL_ARRAY" && raw.starts_with('{');
                        if wrap {
                            output.write_char('[').map_err(write_error)?;
                        }
                        write_container(raw, legacy, &mut output).map_err(write_error)?;
                        if wrap {
                            output.write_char(']').map_err(write_error)?;
                        }
                        super::check_string_capacity(output.values_slice().len(), 0)?;
                        output.append_value("");
                    }
                    Ok(value) if wrapper != "WITHOUT_ARRAY" => {
                        output.write_char('[').map_err(write_error)?;
                        write_scalar(value, legacy, &mut output).map_err(write_error)?;
                        output.write_char(']').map_err(write_error)?;
                        super::check_string_capacity(output.values_slice().len(), 0)?;
                        output.append_value("");
                    }
                    Ok(_) => output.append_option(if path.lax { empty } else { error }),
                }
            }
            finish(output, scalar)
        })
    }
}

fn policy(value: &ColumnarValue) -> Result<Option<&'static str>> {
    match literal(value)? {
        Some("NULL") => Ok(None),
        Some("EMPTY_ARRAY") => Ok(Some("[]")),
        Some("EMPTY_OBJECT") => Ok(Some("{}")),
        _ => exec_err!("JSON_QUERY retains generated evaluation for ERROR policies"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    fn invoke(input: ColumnarValue, path: &str, wrapper: &str) -> ColumnarValue {
        let rows = match &input {
            ColumnarValue::Array(input) => input.len(),
            _ => 1,
        };
        let mut args = vec![input];
        args.extend(
            [path, wrapper, "EMPTY_ARRAY", "EMPTY_OBJECT", "13.0"]
                .map(|value| ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.into())))),
        );
        function()
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
            })
            .unwrap()
    }

    #[test]
    fn containers_sort_utf16_keys_keep_last_duplicates_and_normalize_scalars() {
        let input = r#"{"a":{"z":1.2300,"a":-0,"z":2.5000,"\uE000":true,"\uD83D\uDE00":"\uD800"}}"#;
        let value = invoke(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(input.into()))),
            "strict $.a",
            "WITHOUT_ARRAY",
        );
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) = value else {
            panic!("expected scalar string")
        };
        assert_eq!(
            value,
            "{\"a\":0,\"z\":2.5000,\"\u{1f600}\":\"?\",\"\u{e000}\":true}"
        );
    }

    #[test]
    fn wrappers_and_policies_preserve_sliced_nulls_and_empty_arrays() {
        let input = StringArray::from(vec![
            Some("skip"),
            None,
            Some("{}"),
            Some(r#"{"a":null}"#),
            Some(r#"{"a":17}"#),
            Some(r#"{"a":[]}"#),
            Some(r#"{"a":{}}"#),
            Some("bad"),
        ]);
        for (wrapper, expected) in [
            (
                "WITHOUT_ARRAY",
                vec![
                    None,
                    Some("[]"),
                    Some("[]"),
                    Some("[]"),
                    Some("[]"),
                    Some("{}"),
                    Some("[]"),
                ],
            ),
            (
                "CONDITIONAL_ARRAY",
                vec![
                    None,
                    Some("[]"),
                    Some("[]"),
                    Some("[17]"),
                    Some("[]"),
                    Some("[{}]"),
                    Some("[]"),
                ],
            ),
            (
                "UNCONDITIONAL_ARRAY",
                vec![
                    None,
                    Some("[]"),
                    Some("[]"),
                    Some("[17]"),
                    Some("[[]]"),
                    Some("[{}]"),
                    Some("[]"),
                ],
            ),
        ] {
            let ColumnarValue::Array(actual) = invoke(
                ColumnarValue::Array(Arc::new(input.slice(1, 7))),
                "lax $.a",
                wrapper,
            ) else {
                panic!("expected array")
            };
            assert_eq!(
                as_string_array(&actual).unwrap(),
                &StringArray::from(expected)
            );
            actual.to_data().validate_full().unwrap();
        }
        let ColumnarValue::Array(actual) = invoke(
            ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new()))),
            "$",
            "WITHOUT_ARRAY",
        ) else {
            panic!("expected array")
        };
        assert!(actual.is_empty());
    }
}
