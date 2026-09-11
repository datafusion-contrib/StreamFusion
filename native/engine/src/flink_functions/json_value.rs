use super::json_path::{with_reader, Path, Value};
use arrow::array::{Array, ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(super) enum ReturnType {
    Varchar,
    Boolean,
    Integer,
    Double,
}

impl ReturnType {
    fn data_type(self) -> DataType {
        match self {
            Self::Varchar => DataType::Utf8,
            Self::Boolean => DataType::Boolean,
            Self::Integer => DataType::Int32,
            Self::Double => DataType::Float64,
        }
    }
}

pub(super) fn function() -> ScalarUDF {
    typed_function(ReturnType::Varchar)
}

pub(super) fn typed_function(return_type: ReturnType) -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonValue {
        return_type,
        signature: Signature::exact(vec![DataType::Utf8; 7], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonValue {
    return_type: ReturnType,
    signature: Signature,
}

impl ScalarUDFImpl for JsonValue {
    fn name(&self) -> &str {
        match self.return_type {
            ReturnType::Varchar => "flink_json_value",
            ReturnType::Boolean => "flink_json_value_boolean",
            ReturnType::Integer => "flink_json_value_integer",
            ReturnType::Double => "flink_json_value_double",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input, path, empty, empty_default, error, error_default, unicode] =
            args.args.as_slice()
        else {
            return exec_err!("JSON_VALUE expects input, path and EMPTY/ERROR policies");
        };
        let path_text = literal(path)?.ok_or_else(|| {
            datafusion::common::exec_datafusion_err!("JSON_VALUE requires a path")
        })?;
        let path = Path::parse(path_text, literal(unicode)?.unwrap_or("")).ok_or_else(|| {
            datafusion::common::exec_datafusion_err!("Unsupported JSON_VALUE path")
        })?;
        let empty = Behavior::new(empty, empty_default, self.return_type)?;
        let error = Behavior::new(error, error_default, self.return_type)?;
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let input = as_string_array(&input)?;
        let mut output = Output::new(self.return_type, input.len());
        with_reader(|reader| {
            for document in input.iter() {
                let Some(document) = document else {
                    output.append(Value::Null)?;
                    continue;
                };
                let value = match reader.read(&path, document) {
                    Ok(Value::Missing | Value::Null) => empty.apply("EMPTY")?,
                    Ok(Value::Container) if path.lax => empty.apply("EMPTY")?,
                    Ok(Value::Container) | Err(()) => error.apply("ERROR")?,
                    Ok(value) => value,
                };
                // Flink casts the returned Java object after applying EMPTY/ERROR policies.
                // A scalar type mismatch must therefore fail, even with NULL ON ERROR.
                output.append(value)?;
            }
            finish(output.finish(), scalar)
        })
    }
}

enum Behavior<'a> {
    Null,
    Error,
    Default(Value<'a>),
}

impl<'a> Behavior<'a> {
    fn new(
        behavior: &'a ColumnarValue,
        default: &'a ColumnarValue,
        return_type: ReturnType,
    ) -> Result<Self> {
        match literal(behavior)? {
            Some("NULL") => Ok(Self::Null),
            Some("ERROR") => Ok(Self::Error),
            Some("DEFAULT") => {
                let value = match (return_type, literal(default)?) {
                    (_, None) => Value::Null,
                    (ReturnType::Varchar, Some(value)) => Value::DecodedString(value),
                    (ReturnType::Boolean, Some("true")) => Value::Boolean(true),
                    (ReturnType::Boolean, Some("false")) => Value::Boolean(false),
                    (ReturnType::Integer, Some(value)) if value.parse::<i32>().is_ok() => {
                        Value::Number(value)
                    }
                    _ => return exec_err!("Unsupported JSON_VALUE DEFAULT type"),
                };
                Ok(Self::Default(value))
            }
            _ => exec_err!("Unsupported JSON_VALUE policy"),
        }
    }

    fn apply(&self, mode: &str) -> Result<Value<'_>> {
        match self {
            Self::Null => Ok(Value::Null),
            Self::Error => exec_err!("JSON_VALUE {mode} result is not allowed"),
            Self::Default(value) => Ok(*value),
        }
    }
}

enum Output {
    Varchar(StringBuilder),
    Boolean(BooleanBuilder),
    Integer(Int32Builder),
    Double(Float64Builder),
}

impl Output {
    fn new(return_type: ReturnType, rows: usize) -> Self {
        match return_type {
            ReturnType::Varchar => Self::Varchar(StringBuilder::with_capacity(rows, rows * 8)),
            ReturnType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(rows)),
            ReturnType::Integer => Self::Integer(Int32Builder::with_capacity(rows)),
            ReturnType::Double => Self::Double(Float64Builder::with_capacity(rows)),
        }
    }

    fn append(&mut self, value: Value<'_>) -> Result<()> {
        match (self, value) {
            (Self::Varchar(output), value) => output.append_option(value.text()),
            (Self::Boolean(output), Value::Null) => output.append_null(),
            (Self::Integer(output), Value::Null) => output.append_null(),
            (Self::Double(output), Value::Null) => output.append_null(),
            (Self::Boolean(output), Value::Boolean(value)) => output.append_value(value),
            (Self::Integer(output), Value::Number(raw)) if !is_decimal(raw) => {
                output.append_value(raw.parse::<i32>().map_err(|_| type_error("INTEGER"))?);
            }
            (Self::Double(output), Value::Number(raw)) if is_decimal(raw) => {
                output.append_value(decimal_double(raw)?);
            }
            (Self::Boolean(_), _) => return Err(type_error("BOOLEAN")),
            (Self::Integer(_), _) => return Err(type_error("INTEGER")),
            (Self::Double(_), _) => return Err(type_error("DOUBLE")),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Varchar(mut output) => Arc::new(output.finish()),
            Self::Boolean(mut output) => Arc::new(output.finish()),
            Self::Integer(mut output) => Arc::new(output.finish()),
            Self::Double(mut output) => Arc::new(output.finish()),
        }
    }
}

fn is_decimal(raw: &str) -> bool {
    raw.contains(['.', 'e', 'E'])
}

fn type_error(return_type: &str) -> datafusion::common::DataFusionError {
    datafusion::common::exec_datafusion_err!(
        "JSON_VALUE RETURNING {return_type}: incompatible JSON scalar type"
    )
}

fn decimal_double(raw: &str) -> Result<f64> {
    // Parsing has already checked Jackson's number/BigDecimal constraints. BigDecimal
    // has no signed zero, but rounding a negative nonzero value may still underflow to -0.
    let mantissa = raw.split(['e', 'E']).next().unwrap_or(raw);
    if !mantissa.bytes().any(|byte| matches!(byte, b'1'..=b'9')) {
        return Ok(0.0);
    }
    raw.parse::<f64>().map_err(|_| type_error("DOUBLE"))
}

pub(super) fn literal(value: &ColumnarValue) -> Result<Option<&str>> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(value.as_deref()),
        _ => exec_err!("SQL/JSON path and policies must be string literals"),
    }
}

pub(super) fn finish(output: ArrayRef, scalar: bool) -> Result<ColumnarValue> {
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &output, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;

    fn invoke(input: ColumnarValue, empty: &str, error: &str) -> Result<ColumnarValue> {
        let rows = match &input {
            ColumnarValue::Array(array) => array.len(),
            _ => 1,
        };
        let mut args = vec![input];
        args.extend(
            ["lax $.a", empty, "empty", error, "error", "13.0"]
                .map(|s| ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.into())))),
        );
        function().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        })
    }

    #[test]
    fn sliced_nullable_inputs_and_empty_batches() {
        let input = StringArray::from(vec![
            Some("unused"),
            Some(r#"{"a":"ok"}"#),
            None,
            Some("{}"),
            Some("null"),
            Some("tail"),
        ]);
        let input = input.slice(1, 4);
        let ColumnarValue::Array(output) =
            invoke(ColumnarValue::Array(Arc::new(input)), "DEFAULT", "DEFAULT").unwrap()
        else {
            panic!("expected array")
        };
        output.to_data().validate_full().unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec![Some("ok"), None, Some("empty"), Some("error")])
        );
        let empty = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let ColumnarValue::Array(output) =
            invoke(ColumnarValue::Array(empty), "ERROR", "ERROR").unwrap()
        else {
            panic!("expected array")
        };
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn scalar_null_errors_and_arity() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        assert!(matches!(
            invoke(input, "ERROR", "ERROR").unwrap(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
        ));
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("{}".into())));
        assert!(invoke(input, "ERROR", "DEFAULT")
            .unwrap_err()
            .to_string()
            .contains("JSON_VALUE EMPTY"));
        assert!(function().coerce_types(&[]).is_err());
    }
}

#[cfg(test)]
mod returning_tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::Field;

    fn invoke(kind: ReturnType, input: ColumnarValue) -> Result<ColumnarValue> {
        let rows = match &input {
            ColumnarValue::Array(array) => array.len(),
            _ => 1,
        };
        let mut args = vec![input];
        args.extend(
            ["lax $", "NULL", "unused", "NULL", "unused", "13.0"]
                .map(|s| ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.into())))),
        );
        typed_function(kind).invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", kind.data_type(), true)),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        })
    }

    #[test]
    fn primitive_output_arrays_preserve_slices_nulls_and_empty_batches() {
        for (kind, document, expected) in [
            (
                ReturnType::Boolean,
                "false",
                Arc::new(BooleanArray::from(vec![None, Some(false), None])) as ArrayRef,
            ),
            (
                ReturnType::Integer,
                "-2147483648",
                Arc::new(Int32Array::from(vec![None, Some(i32::MIN), None])) as ArrayRef,
            ),
            (
                ReturnType::Double,
                "-0.0",
                Arc::new(Float64Array::from(vec![None, Some(0.0), None])) as ArrayRef,
            ),
        ] {
            let input = StringArray::from(vec![Some("unused"), None, Some(document), Some("{}")]);
            let ColumnarValue::Array(output) =
                invoke(kind, ColumnarValue::Array(Arc::new(input.slice(1, 3)))).unwrap()
            else {
                panic!("array")
            };
            assert_eq!(output.as_ref(), expected.as_ref());
            output.to_data().validate_full().unwrap();
            let ColumnarValue::Array(empty) =
                invoke(kind, ColumnarValue::Array(Arc::new(input.slice(0, 0)))).unwrap()
            else {
                panic!("array")
            };
            assert!(empty.is_empty());
            assert!(typed_function(kind).coerce_types(&[]).is_err());
        }
    }

    #[test]
    fn wrong_scalar_types_fail_instead_of_becoming_null() {
        for (kind, documents) in [
            (ReturnType::Boolean, vec!["1", "\"true\""]),
            (
                ReturnType::Integer,
                vec!["1.0", "2147483648", "-2147483649", "\"12\""],
            ),
            (ReturnType::Double, vec!["1", "\"1.0\""]),
        ] {
            for document in documents {
                let error = invoke(
                    kind,
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(document.into()))),
                )
                .unwrap_err();
                assert!(error.to_string().contains("incompatible JSON scalar"));
            }
        }
    }

    #[test]
    fn bigdecimal_zero_sign_differs_from_underflow() {
        for raw in ["-0.0", "-0e999999999", "-0.00e-999999999"] {
            assert_eq!(decimal_double(raw).unwrap().to_bits(), 0.0f64.to_bits());
        }
        assert_eq!(
            decimal_double("-1e-400").unwrap().to_bits(),
            (-0.0f64).to_bits()
        );
        assert_eq!(decimal_double("1e309").unwrap(), f64::INFINITY);
        assert_eq!(decimal_double("-1e309").unwrap(), f64::NEG_INFINITY);
    }
}
