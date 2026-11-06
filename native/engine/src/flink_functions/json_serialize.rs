use std::fmt::{self, Write};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, StringBuilder,
};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonString {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonString {
    signature: Signature,
}

impl ScalarUDFImpl for JsonString {
    fn name(&self) -> &str {
        "flink_json_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 1 || !super::json_container::supported(&types[0]) {
            return exec_err!("JSON_STRING requires a verified scalar, ARRAY, or ROW input");
        }
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.coerce_types(types)?;
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("JSON_STRING expects one input");
        };
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let values = if supported(input.data_type()) {
            JsonInput::Scalar(JsonColumn::new(&input)?)
        } else {
            JsonInput::Container(super::json_container::JsonContainer::new(&input)?)
        };
        let nulls = input.logical_nulls();
        let mut output = StringBuilder::with_capacity(input.len(), input.len() * 16);
        for row in 0..input.len() {
            if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                output.append_null();
            } else {
                values.write(row, &mut output).map_err(write_error)?;
                super::check_string_capacity(output.values_slice().len(), 0)?;
                output.append_value("");
            }
        }
        finish(output, scalar)
    }
}

enum JsonInput<'a> {
    Scalar(JsonColumn<'a>),
    Container(super::json_container::JsonContainer<'a>),
}

impl JsonInput<'_> {
    fn write(&self, row: usize, out: &mut impl Write) -> fmt::Result {
        match self {
            Self::Scalar(values) => values.write(row, out),
            Self::Container(values) => values.write(row, out),
        }
    }
}

pub(super) fn supported(datatype: &DataType) -> bool {
    matches!(
        datatype,
        DataType::Null
            | DataType::Utf8
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, 0..=38)
    )
}

/// Downcast each input once per batch; rows write directly into the output string builder.
pub(super) enum JsonColumn<'a> {
    Null,
    String(&'a StringArray),
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Decimal {
        values: &'a Decimal128Array,
        scale: usize,
        divisor: u128,
    },
}

impl<'a> JsonColumn<'a> {
    pub(super) fn new(input: &'a ArrayRef) -> Result<Self> {
        use datafusion::common::cast::*;
        Ok(match input.data_type() {
            DataType::Null => Self::Null,
            DataType::Utf8 => Self::String(as_string_array(input)?),
            DataType::Boolean => Self::Boolean(as_boolean_array(input)?),
            DataType::Int8 => Self::Int8(as_int8_array(input)?),
            DataType::Int16 => Self::Int16(as_int16_array(input)?),
            DataType::Int32 => Self::Int32(as_int32_array(input)?),
            DataType::Int64 => Self::Int64(as_int64_array(input)?),
            DataType::Decimal128(_, scale @ 0..=38) => Self::Decimal {
                values: as_decimal128_array(input)?,
                scale: *scale as usize,
                divisor: 10u128.pow(*scale as u32),
            },
            other => return exec_err!("Unverified JSON scalar type: {other}"),
        })
    }

    pub(super) fn write(&self, row: usize, out: &mut impl Write) -> fmt::Result {
        match self {
            Self::Null => out.write_str("null"),
            Self::String(values) => crate::json_string::write_json_string(out, values.value(row)),
            Self::Boolean(values) => {
                out.write_str(if values.value(row) { "true" } else { "false" })
            }
            Self::Int8(values) => write!(out, "{}", values.value(row)),
            Self::Int16(values) => write!(out, "{}", values.value(row)),
            Self::Int32(values) => write!(out, "{}", values.value(row)),
            Self::Int64(values) => write!(out, "{}", values.value(row)),
            Self::Decimal {
                values,
                scale,
                divisor,
            } => write_decimal(out, values.value(row), *scale, *divisor),
        }
    }
}

fn write_decimal(out: &mut impl Write, value: i128, scale: usize, divisor: u128) -> fmt::Result {
    let magnitude = value.unsigned_abs();
    let digits = magnitude.checked_ilog10().unwrap_or(0) as usize + 1;
    let exponent = digits as i32 - 1 - scale as i32;
    if value < 0 {
        out.write_char('-')?;
    }
    // BigDecimal.toString retains scale, switching to scientific notation below exponent -6.
    if exponent < -6 {
        let fraction_width = digits - 1;
        let leading_divisor = 10u128.pow(fraction_width as u32);
        write!(out, "{}", magnitude / leading_divisor)?;
        if fraction_width > 0 {
            write!(out, ".{:0fraction_width$}", magnitude % leading_divisor)?;
        }
        write!(out, "E{exponent}")
    } else {
        write!(out, "{}", magnitude / divisor)?;
        if scale > 0 {
            write!(out, ".{:0scale$}", magnitude % divisor)?;
        }
        Ok(())
    }
}

pub(super) fn write_error(error: fmt::Error) -> datafusion::common::DataFusionError {
    datafusion::common::exec_datafusion_err!("JSON serialization failed: {error}")
}

pub(super) fn finish(mut output: StringBuilder, scalar: bool) -> Result<ColumnarValue> {
    let output: ArrayRef = Arc::new(output.finish());
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
    use arrow::datatypes::Field;

    fn invoke(input: ColumnarValue) -> ColumnarValue {
        let rows = match &input {
            ColumnarValue::Array(values) => values.len(),
            _ => 3,
        };
        function()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![input],
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
            })
            .unwrap()
    }

    #[test]
    fn strings_and_nulls_on_sliced_batches() {
        let input =
            StringArray::from(vec![Some("skip"), Some("/\0\u{1f600}"), None, Some("")]).slice(1, 3);
        let ColumnarValue::Array(output) = invoke(ColumnarValue::Array(Arc::new(input))) else {
            panic!("array input must produce an array");
        };
        assert_eq!(
            datafusion::common::cast::as_string_array(&output).unwrap(),
            &StringArray::from(vec![Some("\"/\\u0000\u{1f600}\""), None, Some("\"\"")])
        );
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn scalar_integer_extremes_and_boolean_remain_scalars() {
        for (input, expected) in [
            (ScalarValue::Int8(Some(i8::MIN)), Some("-128")),
            (ScalarValue::Int16(Some(i16::MIN)), Some("-32768")),
            (ScalarValue::Int32(Some(i32::MIN)), Some("-2147483648")),
            (
                ScalarValue::Int64(Some(i64::MIN)),
                Some("-9223372036854775808"),
            ),
            (
                ScalarValue::Int64(Some(i64::MAX)),
                Some("9223372036854775807"),
            ),
            (ScalarValue::Boolean(Some(false)), Some("false")),
            (ScalarValue::Int64(None), None),
        ] {
            let output = invoke(ColumnarValue::Scalar(input));
            assert!(
                matches!(output, ColumnarValue::Scalar(ScalarValue::Utf8(value))
                if value.as_deref() == expected)
            );
        }
    }

    #[test]
    fn unverified_types_and_wrong_arity_return_errors() {
        for types in [vec![], vec![DataType::Float64], vec![DataType::Utf8; 2]] {
            assert!(function().coerce_types(&types).is_err());
        }
    }

    #[test]
    fn decimals_preserve_scale_and_scientific_notation() {
        assert!(matches!(
            invoke(ColumnarValue::Scalar(ScalarValue::Null)),
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
        ));
        for (value, scale, expected) in [
            (12300, 4, "1.2300"),
            (0, 6, "0.000000"),
            (0, 7, "0E-7"),
            (1, 7, "1E-7"),
            (10, 7, "0.0000010"),
            (-100, 9, "-1.00E-7"),
            (100000, 2, "1000.00"),
            (
                10i128.pow(38) - 1,
                0,
                "99999999999999999999999999999999999999",
            ),
            (
                10i128.pow(38) - 1,
                38,
                "0.99999999999999999999999999999999999999",
            ),
        ] {
            let output = invoke(ColumnarValue::Scalar(ScalarValue::Decimal128(
                Some(value),
                38,
                scale,
            )));
            assert!(
                matches!(output, ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) if value == expected)
            );
        }
        let input = Decimal128Array::from(vec![Some(999), None, Some(0), Some(-100)])
            .with_precision_and_scale(38, 9)
            .unwrap()
            .slice(1, 3);
        let ColumnarValue::Array(output) = invoke(ColumnarValue::Array(Arc::new(input))) else {
            panic!("array input must remain an array");
        };
        assert_eq!(
            datafusion::common::cast::as_string_array(&output).unwrap(),
            &StringArray::from(vec![None, Some("0E-9"), Some("-1.00E-7")])
        );
        output.to_data().validate_full().unwrap();
    }
}
