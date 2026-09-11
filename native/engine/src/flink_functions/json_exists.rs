use super::json_path::{with_reader, Path, Value};
use super::json_value::{finish, literal};
use arrow::array::{Array, BooleanBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonExists {
        signature: Signature::exact(vec![DataType::Utf8; 4], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExists {
    signature: Signature,
}

impl ScalarUDFImpl for JsonExists {
    fn name(&self) -> &str {
        "flink_json_exists"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input, path, error, unicode] = args.args.as_slice() else {
            return exec_err!("JSON_EXISTS expects input, path and ON ERROR policy");
        };
        let path_text = literal(path)?.ok_or_else(|| {
            datafusion::common::exec_datafusion_err!("JSON_EXISTS requires a path")
        })?;
        let path = Path::parse(path_text, literal(unicode)?.unwrap_or("")).ok_or_else(|| {
            datafusion::common::exec_datafusion_err!("Unsupported JSON_EXISTS path")
        })?;
        let error = match literal(error)? {
            Some("FALSE") => Ok(Some(false)),
            Some("TRUE") => Ok(Some(true)),
            Some("UNKNOWN") => Ok(None),
            Some("ERROR") => Err(()),
            _ => return exec_err!("Unsupported JSON_EXISTS ON ERROR policy"),
        };
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let input = as_string_array(&input)?;
        let mut output = BooleanBuilder::with_capacity(input.len());
        with_reader(|reader| {
            for document in input.iter() {
                let Some(document) = document else {
                    output.append_null();
                    continue;
                };
                let value = match reader.read(&path, document) {
                    Ok(Value::Missing | Value::Null) => Some(false),
                    Ok(_) => Some(true),
                    Err(()) => error.map_err(|()| {
                        datafusion::common::exec_datafusion_err!(
                            "JSON_EXISTS ERROR result is not allowed"
                        )
                    })?,
                };
                output.append_option(value);
            }
            finish(Arc::new(output.finish()), scalar)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::common::ScalarValue;

    fn invoke(input: ColumnarValue, policy: &str) -> Result<ColumnarValue> {
        let rows = match &input {
            ColumnarValue::Array(array) => array.len(),
            _ => 1,
        };
        let mut args = vec![input];
        args.extend(
            ["strict $.a", policy, "13.0"]
                .map(|s| ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.into())))),
        );
        function().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        })
    }

    #[test]
    fn sliced_input_preserves_nulls_and_unknown_errors() {
        let input = StringArray::from(vec![
            Some("unused"),
            Some(r#"{"a":[]}"#),
            None,
            Some("{}"),
            Some(r#"{"a":false}"#),
            Some("tail"),
        ]);
        let ColumnarValue::Array(output) =
            invoke(ColumnarValue::Array(Arc::new(input.slice(1, 4))), "UNKNOWN").unwrap()
        else {
            panic!("expected array")
        };
        output.to_data().validate_full().unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );
    }

    #[test]
    fn scalar_error_policy_empty_batch_and_arity() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("{}".into())));
        assert!(matches!(
            invoke(input.clone(), "TRUE").unwrap(),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        ));
        assert!(invoke(input, "ERROR").is_err());
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        assert!(matches!(
            invoke(input, "ERROR").unwrap(),
            ColumnarValue::Scalar(ScalarValue::Boolean(None))
        ));
        let empty = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let ColumnarValue::Array(output) = invoke(ColumnarValue::Array(empty), "ERROR").unwrap()
        else {
            panic!("expected array")
        };
        assert_eq!(output.len(), 0);
        assert!(function().coerce_types(&[]).is_err());
    }
}
