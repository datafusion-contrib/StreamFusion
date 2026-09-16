use std::fmt::Write;
use std::sync::Arc;

use arrow::array::{Int32Array, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::as_int32_array, cast::as_string_array, exec_datafusion_err, exec_err, Result, ScalarValue,
};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct IntegerStringCast {
    length: Option<usize>,
    signature: Signature,
}

impl IntegerStringCast {
    pub(crate) fn parse() -> Self {
        Self {
            length: None,
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }

    pub(crate) fn format(length: usize) -> Self {
        Self {
            length: Some(length),
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IntegerStringCast {
    fn name(&self) -> &str {
        if self.length.is_some() {
            "flink_int_to_string"
        } else {
            "flink_string_to_int"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let (source, target) = if self.length.is_some() {
            (DataType::Int32, DataType::Utf8)
        } else {
            (DataType::Utf8, DataType::Int32)
        };
        if args != [source] {
            return exec_err!("{} received unexpected types: {args:?}", self.name());
        }
        Ok(target)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("{} requires one argument", self.name());
        };
        match (self.length, input) {
            (None, ColumnarValue::Scalar(ScalarValue::Utf8(value))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int32(value.as_deref().map(parse_int).transpose()?),
            )),
            (Some(length), ColumnarValue::Scalar(ScalarValue::Int32(value))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Utf8(value.map(|value| {
                    let mut text = value.to_string();
                    text.truncate(length.min(text.len()));
                    text
                }))),
            ),
            (None, ColumnarValue::Array(array)) => {
                let input = as_string_array(array)?;
                let output = input
                    .iter()
                    .map(|value| value.map(parse_int).transpose())
                    .collect::<Result<Int32Array>>()?;
                Ok(ColumnarValue::Array(Arc::new(output)))
            }
            (Some(length), ColumnarValue::Array(array)) => {
                let input = as_int32_array(array)?;
                let mut output = StringBuilder::with_capacity(
                    input.len(),
                    input.len().saturating_mul(length.min(11)),
                );
                let mut text = String::with_capacity(11);
                for value in input.iter() {
                    match value {
                        Some(value) => {
                            text.clear();
                            write!(text, "{value}")?;
                            super::check_string_capacity(
                                output.values_slice().len(),
                                length.min(text.len()),
                            )?;
                            output.append_value(&text[..length.min(text.len())]);
                        }
                        None => output.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(output.finish())))
            }
            _ => exec_err!(
                "{} received unexpected input: {:?}",
                self.name(),
                input.data_type()
            ),
        }
    }
}

fn parse_int(input: &str) -> Result<i32> {
    // Flink 2.2.1 StringToNumericPrimitiveCastRule uses BinaryStringData.trim(), not
    // Java/Rust whitespace trimming, followed by BinaryStringDataUtil.toInt().
    let input = input.trim_matches(' ');
    let fail = |reason| exec_datafusion_err!("For input string: '{input}'. {reason}");
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return Err(fail("Input is empty."));
    }
    let negative = bytes[0] == b'-';
    let signed = negative || bytes[0] == b'+';
    if signed && bytes.len() == 1 {
        return Err(fail("Input has only positive or negative symbol."));
    }
    // Accumulate negatively so MIN_VALUE is representable. Flink accepts a decimal point
    // even without digits on either side, and validates but discards fractional digits.
    let mut result = 0i32;
    let mut fractional = false;
    for &byte in &bytes[usize::from(signed)..] {
        if byte == b'.' && !fractional {
            fractional = true;
            continue;
        }
        if !byte.is_ascii_digit() {
            return Err(fail("Invalid character found."));
        }
        if !fractional {
            result = result
                .checked_mul(10)
                .and_then(|value| value.checked_sub(i32::from(byte - b'0')))
                .ok_or_else(|| fail("Overflow."))?;
        }
    }
    if negative {
        Ok(result)
    } else {
        result.checked_neg().ok_or_else(|| fail("Overflow."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    fn invoke(cast: &IntegerStringCast, input: ColumnarValue, rows: usize) -> ColumnarValue {
        let output = cast.return_type(&[input.data_type()]).unwrap();
        cast.invoke_with_args(ScalarFunctionArgs {
            arg_fields: vec![Arc::new(Field::new("input", input.data_type(), true))],
            args: vec![input],
            number_rows: rows,
            return_field: Arc::new(Field::new("cast", output, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    }

    #[test]
    fn flink_integer_grammar_and_bounds() {
        for (text, expected) in [
            ("0", 0),
            ("-0", 0),
            (" +00042 ", 42),
            ("-12.9", -12),
            ("12.9", 12),
            (".5", 0),
            (".", 0),
            ("+.", 0),
            ("-.", 0),
            ("1.", 1),
            ("2147483647.99", i32::MAX),
            ("-2147483648.99", i32::MIN),
        ] {
            assert_eq!(parse_int(text).unwrap(), expected, "{text:?}");
        }
        assert_eq!(parse_int(&format!("{}1", "0".repeat(1000))).unwrap(), 1);
        for (text, reason) in [
            ("", "Input is empty."),
            (" ", "Input is empty."),
            ("+", "Input has only positive or negative symbol."),
            ("-", "Input has only positive or negative symbol."),
            ("2147483648", "Overflow."),
            ("-2147483649", "Overflow."),
            ("999999999999999", "Overflow."),
            ("1e2", "Invalid character found."),
            ("1.2.3", "Invalid character found."),
            ("\t42\n", "Invalid character found."),
            ("\042", "Invalid character found."),
            ("\u{a0}42", "Invalid character found."),
            ("\u{3000}42", "Invalid character found."),
            ("\u{ff11}\u{ff12}", "Invalid character found."),
            ("1 2", "Invalid character found."),
            ("--1", "Invalid character found."),
            ("1.0x", "Invalid character found."),
        ] {
            assert!(
                parse_int(text).unwrap_err().to_string().contains(reason),
                "{text:?}"
            );
        }
    }

    #[test]
    fn sliced_arrays_nulls_and_empty_batches() {
        let source = StringArray::from(vec![Some("bad"), Some("42.9"), None, Some("-2147483648")]);
        let output = invoke(
            &IntegerStringCast::parse(),
            ColumnarValue::Array(Arc::new(source.slice(1, 3))),
            3,
        );
        assert_eq!(
            as_int32_array(&output.into_array(3).unwrap()).unwrap(),
            &Int32Array::from(vec![Some(42), None, Some(i32::MIN)])
        );
        for length in [1, 2, 11, i32::MAX as usize] {
            let source =
                Int32Array::from(vec![Some(123), Some(0), Some(-123), None, Some(i32::MIN)])
                    .slice(1, 4);
            let output = invoke(
                &IntegerStringCast::format(length),
                ColumnarValue::Array(Arc::new(source)),
                4,
            )
            .into_array(4)
            .unwrap();
            let expected = [Some("0"), Some("-123"), None, Some("-2147483648")]
                .map(|s| s.map(|s| &s[..length.min(s.len())]));
            assert_eq!(
                as_string_array(&output).unwrap(),
                &StringArray::from(expected.to_vec())
            );
            output.to_data().validate_full().unwrap();
        }
        for (cast, input) in [
            (
                IntegerStringCast::parse(),
                ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new()))),
            ),
            (
                IntegerStringCast::format(2),
                ColumnarValue::Array(Arc::new(Int32Array::from(Vec::<Option<i32>>::new()))),
            ),
        ] {
            assert_eq!(invoke(&cast, input, 0).into_array(0).unwrap().len(), 0);
        }
    }

    #[test]
    fn scalars_stay_scalar() {
        for (value, expected) in [(Some(" -17.5 "), Some(-17)), (None, None)] {
            let ColumnarValue::Scalar(output) = invoke(
                &IntegerStringCast::parse(),
                ColumnarValue::Scalar(ScalarValue::Utf8(value.map(str::to_owned))),
                4096,
            ) else {
                panic!("scalar cast returned an array");
            };
            assert_eq!(output, ScalarValue::Int32(expected));
        }
        for (value, expected) in [(Some(i32::MIN), Some("-2")), (None, None)] {
            let ColumnarValue::Scalar(output) = invoke(
                &IntegerStringCast::format(2),
                ColumnarValue::Scalar(ScalarValue::Int32(value)),
                4096,
            ) else {
                panic!("scalar cast returned an array");
            };
            assert_eq!(output, ScalarValue::Utf8(expected.map(str::to_owned)));
        }
    }
}
