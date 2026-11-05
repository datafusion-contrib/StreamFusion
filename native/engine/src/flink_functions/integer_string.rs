use arrow::array::{Array, ArrayRef, PrimitiveBuilder, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{
    cast::{as_int64_array, as_string_array},
    exec_err, Result,
};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(crate) fn parse_function(target: DataType, null_on_error: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(IntegerString {
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        operation: Operation::Parse {
            target,
            null_on_error,
        },
    })
}

pub(crate) fn format_function(length: usize, pad: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(IntegerString {
        signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        operation: Operation::Format { length, pad },
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum Operation {
    Parse {
        target: DataType,
        null_on_error: bool,
    },
    Format {
        length: usize,
        pad: bool,
    },
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntegerString {
    signature: Signature,
    operation: Operation,
}

impl ScalarUDFImpl for IntegerString {
    fn name(&self) -> &str {
        match self.operation {
            Operation::Parse { .. } => "flink_string_to_integer",
            Operation::Format { .. } => "flink_integer_to_string",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match &self.operation {
            Operation::Parse { target, .. } => target.clone(),
            Operation::Format { .. } => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(|arrays| self.cast(arrays), vec![])(
            &args.args,
        )
    }
}

impl IntegerString {
    fn cast(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [input] = args else {
            return exec_err!("Integer/string cast expects one argument");
        };
        match self.operation {
            Operation::Parse {
                ref target,
                null_on_error,
            } => match target {
                DataType::Int8 => parse_array::<Int8Type>(input, null_on_error),
                DataType::Int16 => parse_array::<Int16Type>(input, null_on_error),
                DataType::Int32 => parse_array::<Int32Type>(input, null_on_error),
                DataType::Int64 => parse_array::<Int64Type>(input, null_on_error),
                _ => exec_err!("Unsupported integer target: {target}"),
            },
            Operation::Format { length, pad } => format_array(input, length, pad),
        }
    }
}

fn parse_array<T: ArrowPrimitiveType>(input: &ArrayRef, null_on_error: bool) -> Result<ArrayRef>
where
    T::Native: TryFrom<i64>,
{
    let input = as_string_array(input)?;
    let mut result = PrimitiveBuilder::<T>::with_capacity(input.len());
    for value in input {
        let Some(value) = value else {
            result.append_null();
            continue;
        };
        // BinaryStringData.trim removes only ASCII spaces, not tabs or Unicode whitespace.
        let trimmed = value.trim_matches(' ');
        let parsed =
            parse(trimmed).and_then(|value| T::Native::try_from(value).map_err(|_| "Overflow."));
        match parsed {
            Ok(value) => result.append_value(value),
            Err(_) if null_on_error => result.append_null(),
            Err(reason) => {
                return Err(datafusion::common::DataFusionError::External(Box::new(
                    streamfusion_bridge::FlinkException::number_format(format!(
                        "For input string: '{trimmed}'. {reason}"
                    )),
                )))
            }
        }
    }
    Ok(Arc::new(result.finish()))
}

fn parse(value: &str) -> std::result::Result<i64, &'static str> {
    let mut bytes = value.as_bytes();
    let Some(&first) = bytes.first() else {
        return Err("Input is empty.");
    };
    let negative = first == b'-';
    if negative || first == b'+' {
        bytes = &bytes[1..];
        if bytes.is_empty() {
            return Err("Input has only positive or negative symbol.");
        }
    }
    // Accumulate negatively so MIN_VALUE remains representable. Flink also admits a decimal
    // separator without integral digits and validates, but discards, all fractional digits.
    let mut result = 0i64;
    let mut fraction = false;
    for &byte in bytes {
        if byte == b'.' && !fraction {
            fraction = true;
            continue;
        }
        if !byte.is_ascii_digit() {
            return Err("Invalid character found.");
        }
        if !fraction {
            result = result
                .checked_mul(10)
                .and_then(|v| v.checked_sub(i64::from(byte - b'0')))
                .ok_or("Overflow.")?;
        }
    }
    if negative {
        Ok(result)
    } else {
        result.checked_neg().ok_or("Overflow.")
    }
}

fn format_array(input: &ArrayRef, length: usize, pad: bool) -> Result<ArrayRef> {
    let input = as_int64_array(input)?;
    let mut offsets = Vec::with_capacity(input.len() + 1);
    offsets.push(0_i32);
    let mut bytes = Vec::with_capacity(input.len().saturating_mul(8));
    let mut digits = itoa::Buffer::new();
    for value in input {
        if let Some(value) = value {
            let text = digits.format(value);
            let text = &text[..text.len().min(length)];
            let width = if pad { length } else { text.len() };
            super::check_string_capacity(bytes.len(), width)?;
            let end = bytes.len() + width;
            bytes.extend_from_slice(text.as_bytes());
            bytes.resize(end, b' ');
        }
        offsets.push(bytes.len() as i32);
    }
    Ok(Arc::new(StringArray::new(
        OffsetBuffer::new(offsets.into()),
        bytes.into(),
        input.nulls().cloned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};

    #[test]
    fn sliced_arrays_preserve_nulls_and_widths() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("unused"),
            Some(" -2147483648.9 "),
            None,
            Some("+."),
            Some("2147483647"),
        ]));
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(0),
            Some(i32::MAX),
        ]));
        assert_eq!(
            &parse_array::<Int32Type>(&input.slice(1, 4), false).unwrap(),
            &expected
        );
        let longs: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(7),
            Some(i64::MIN),
            None,
            Some(i64::MAX),
        ]));
        let expected: ArrayRef =
            Arc::new(StringArray::from(vec![Some("-922"), None, Some("9223")]));
        assert_eq!(
            &format_array(&longs.slice(1, 3), 4, false).unwrap(),
            &expected
        );
    }

    #[test]
    fn parser_validates_fraction_sign_and_overflow() {
        for (text, expected) in [
            (".", 0),
            ("-.9", 0),
            ("123.", 123),
            ("-123.9", -123),
            ("9223372036854775807.9", i64::MAX),
            ("-9223372036854775808.9", i64::MIN),
        ] {
            assert_eq!(parse(text), Ok(expected), "{text}");
        }
        for text in [
            "",
            "+",
            "--1",
            "1e2",
            "1.2.3",
            "1.2x",
            "١",
            "\t1",
            "1\n",
            "1\u{a0}",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
        let invalid: ArrayRef = Arc::new(StringArray::from(vec!["128", "-129", "invalid"]));
        assert!(parse_array::<Int8Type>(&invalid, false).is_err());
        assert_eq!(
            parse_array::<Int8Type>(&invalid, true)
                .unwrap()
                .null_count(),
            3
        );
    }

    #[test]
    fn empty_and_all_null_batches_retain_the_declared_type() {
        for len in [0, 3] {
            let input: ArrayRef = Arc::new(StringArray::new_null(len));
            let result = parse_array::<Int16Type>(&input, false).unwrap();
            assert_eq!(result.data_type(), &DataType::Int16);
            assert_eq!(result.null_count(), len);
        }
    }

    #[test]
    fn formatting_matches_display_truncation_and_padding_with_shared_sliced_nulls() {
        let domain = [i64::MIN, i64::MAX, -1, 0, 1, -128, 127, -32768, 32767];
        let source = Int64Array::from_iter(
            (0..151).map(|i| (i % 11 != 0).then_some(domain[i % domain.len()])),
        )
        .slice(7, 137);
        let input: ArrayRef = Arc::new(source.clone());
        for width in [0, 1, 4, 19, 20, 25, 256, usize::MAX] {
            for pad in [false, true] {
                if pad && width == usize::MAX {
                    continue;
                }
                let expected: Vec<_> = source
                    .iter()
                    .map(|value| {
                        value.map(|value| {
                            let mut text = value.to_string();
                            text.truncate(width);
                            if pad {
                                text.extend(std::iter::repeat_n(' ', width - text.len()));
                            }
                            text
                        })
                    })
                    .collect();
                let output = format_array(&input, width, pad).unwrap();
                let output = as_string_array(&output).unwrap();
                assert_eq!(
                    output
                        .iter()
                        .map(|v| v.map(str::to_owned))
                        .collect::<Vec<_>>(),
                    expected
                );
                assert_eq!(output.nulls(), source.nulls());
                assert_eq!(
                    output.nulls().unwrap().buffer().as_ptr(),
                    source.nulls().unwrap().buffer().as_ptr()
                );
            }
        }
        for len in [0, 137] {
            let input: ArrayRef = Arc::new(Int64Array::new_null(len));
            let output = format_array(&input, 25, true).unwrap();
            assert_eq!(output.len(), len);
            assert_eq!(output.null_count(), len);
            assert_eq!(output.data_type(), &DataType::Utf8);
        }
    }
}
