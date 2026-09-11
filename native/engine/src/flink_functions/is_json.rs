use super::json_path::{with_reader, Path, Value};
use super::json_value::{finish, literal};
use arrow::array::{Array, BooleanBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_datafusion_err, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(super) enum JsonType {
    Value,
    Object,
    Array,
    Scalar,
}

pub(super) fn function(kind: JsonType) -> ScalarUDF {
    ScalarUDF::new_from_impl(IsJson {
        kind,
        signature: Signature::exact(vec![DataType::Utf8; 2], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IsJson {
    kind: JsonType,
    signature: Signature,
}

impl ScalarUDFImpl for IsJson {
    fn name(&self) -> &str {
        match self.kind {
            JsonType::Value => "flink_is_json_value",
            JsonType::Object => "flink_is_json_object",
            JsonType::Array => "flink_is_json_array",
            JsonType::Scalar => "flink_is_json_scalar",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input, unicode] = args.args.as_slice() else {
            return exec_err!("IS JSON expects input and a runtime profile");
        };
        let root = Path::parse("$", literal(unicode)?.unwrap_or(""))
            .ok_or_else(|| exec_datafusion_err!("Unsupported IS JSON runtime profile"))?;
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let input = as_string_array(&input)?;
        let mut output = BooleanBuilder::with_capacity(input.len());
        with_reader(|reader| {
            for document in input.iter() {
                let valid =
                    document.is_some_and(|document| match reader.read_document(&root, document) {
                        Err(()) => false,
                        Ok(value) => match self.kind {
                            JsonType::Value => true,
                            JsonType::Scalar => !matches!(value, Value::Container),
                            JsonType::Object | JsonType::Array => {
                                let first = document
                                    .trim_start_matches([' ', '\t', '\r', '\n'])
                                    .as_bytes()
                                    .first()
                                    .copied();
                                matches!(value, Value::Container)
                                    && first
                                        == Some(if self.kind == JsonType::Object {
                                            b'{'
                                        } else {
                                            b'['
                                        })
                            }
                        },
                    });
                // SQL NULL and parse errors are false, whereas JSON null is a valid scalar.
                output.append_value(valid);
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

    fn invoke(kind: JsonType, input: ColumnarValue) -> Result<ColumnarValue> {
        let rows = match &input {
            ColumnarValue::Array(a) => a.len(),
            _ => 1,
        };
        function(kind).invoke_with_args(ScalarFunctionArgs {
            args: vec![
                input,
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("13.0".into()))),
            ],
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Boolean, false)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        })
    }

    #[test]
    fn root_types_include_json_null_and_exclude_sql_null() {
        let input = StringArray::from(vec![
            Some("unused"),
            None,
            Some("null"),
            Some("{}"),
            Some(" \n[]"),
            Some("true"),
            Some("123"),
            Some("\"text\""),
            Some("{\"bad\":[}"),
            Some("tail"),
        ]);
        for (kind, expected) in [
            (
                JsonType::Value,
                vec![false, true, true, true, true, true, true, false],
            ),
            (
                JsonType::Object,
                vec![false, false, true, false, false, false, false, false],
            ),
            (
                JsonType::Array,
                vec![false, false, false, true, false, false, false, false],
            ),
            (
                JsonType::Scalar,
                vec![false, true, false, false, true, true, true, false],
            ),
        ] {
            let ColumnarValue::Array(output) =
                invoke(kind, ColumnarValue::Array(Arc::new(input.slice(1, 8)))).unwrap()
            else {
                panic!("array")
            };
            assert_eq!(output.as_ref(), &BooleanArray::from(expected));
            output.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn scalars_empty_arrays_and_arity() {
        for (input, expected) in [
            (None, false),
            (Some("null"), true),
            (Some("truex"), false),
            (Some("true trailing"), true),
        ] {
            assert!(
                matches!(invoke(JsonType::Value, ColumnarValue::Scalar(ScalarValue::Utf8(input.map(str::to_owned)))).unwrap(),
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))) if value == expected)
            );
        }
        let ColumnarValue::Array(output) = invoke(
            JsonType::Array,
            ColumnarValue::Array(Arc::new(StringArray::from(Vec::<&str>::new()))),
        )
        .unwrap() else {
            panic!("array")
        };
        assert!(output.is_empty());
        assert!(function(JsonType::Value).coerce_types(&[]).is_err());
    }
}
