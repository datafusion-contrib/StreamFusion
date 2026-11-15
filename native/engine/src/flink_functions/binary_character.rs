use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::lpad::Utf16Prefix;

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(BinaryCharacter {
        signature: Signature::one_of(
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryCharacter {
    signature: Signature,
}

impl ScalarUDFImpl for BinaryCharacter {
    fn name(&self) -> &str {
        "flink_binary_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Binary | DataType::FixedSizeBinary(_)]
            | [DataType::Binary | DataType::FixedSizeBinary(_), DataType::Int32] => {
                Ok(DataType::Utf8)
            }
            _ => exec_err!("Binary to STRING expects one binary argument and an optional length"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // A positive length truncates VARCHAR(n); a negative length also pads CHAR(n).
        let length = match args.args.get(1) {
            None => None,
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(length))))
                if *length != 0 && *length != i32::MIN =>
            {
                Some(*length)
            }
            Some(_) => return exec_err!("Binary to CHAR/VARCHAR requires a literal length"),
        };
        let decode = make_bounded(length);
        datafusion::functions::utils::make_scalar_function(decode, vec![])(&args.args[..1])
    }
}

fn make_bounded(length: Option<i32>) -> impl Fn(&[ArrayRef]) -> Result<ArrayRef> {
    move |args| {
        let decoded = cast(args)?;
        match length {
            None => Ok(decoded),
            Some(length) => bound(
                decoded.as_any().downcast_ref::<StringArray>().unwrap(),
                length.unsigned_abs() as usize,
                length < 0,
            ),
        }
    }
}

/// Applies Flink's modern CHAR/VARCHAR rule to JDK-decoded text: keep at most `limit` UTF-16
/// units, then pad CHAR with spaces. A split surrogate pair encodes as '?' at output.
fn bound(input: &StringArray, limit: usize, pad: bool) -> Result<ArrayRef> {
    // UTF-8 never uses fewer bytes than UTF-16 units, so short rows cannot be truncated.
    let fits = |row: usize| input.value_length(row) as usize <= limit;
    let unchanged = if pad {
        (0..input.len()).all(|row| {
            input.is_null(row)
                || input.value_length(row) as usize == limit && input.value(row).is_ascii()
        })
    } else {
        (0..input.len()).all(|row| input.is_null(row) || fits(row))
    };
    if unchanged {
        return Ok(Arc::new(input.clone()));
    }
    let capacity = if pad {
        input.len().saturating_mul(limit)
    } else {
        input.values().len()
    };
    // Write values and offsets directly; NULL slots stay empty and reuse the input validity.
    let mut values = Vec::with_capacity(capacity);
    let mut offsets = Vec::with_capacity(input.len() + 1);
    offsets.push(0_i32);
    for row in 0..input.len() {
        if !input.is_null(row) {
            let value = input.value(row);
            let bytes = value.as_bytes();
            let units = if bytes.len() <= limit && (!pad || value.is_ascii()) {
                values.extend_from_slice(bytes);
                if pad {
                    bytes.len()
                } else {
                    limit
                }
            } else if bytes.len() > limit && bytes[..limit].is_ascii() {
                // An ASCII prefix has one UTF-16 unit per byte.
                values.extend_from_slice(&bytes[..limit]);
                limit
            } else {
                let prefix = Utf16Prefix::new(value, limit);
                values.extend_from_slice(prefix.text.as_bytes());
                // JDK UTF-8 writes the high surrogate left by a split pair as '?'.
                if prefix.split_surrogate {
                    values.push(b'?');
                }
                prefix.units
            };
            if pad {
                values.resize(values.len() + (limit - units), b' ');
            }
        }
        let Ok(offset) = i32::try_from(values.len()) else {
            return exec_err!("Function output exceeds Arrow string capacity");
        };
        offsets.push(offset);
    }
    // SAFETY: rows are whole UTF-8 strings, ASCII prefixes, char-boundary prefixes plus ASCII.
    Ok(Arc::new(unsafe {
        StringArray::new_unchecked(
            OffsetBuffer::new_unchecked(offsets.into()),
            values.into(),
            input.nulls().cloned(),
        )
    }))
}

fn cast(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("Binary to STRING expects one binary argument");
    };
    let binary;
    let input = if let Some(fixed) = input.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if fixed.values().len() > i32::MAX as usize {
            return exec_err!("Binary to STRING exceeds Arrow offset capacity");
        }
        // Preserve the actual sliced bytes and padding without Arrow's fixed-to-variable copy.
        binary = Arc::new(BinaryArray::new(
            OffsetBuffer::from_lengths(std::iter::repeat_n(
                fixed.value_length() as usize,
                fixed.len(),
            )),
            fixed.values().clone(),
            fixed.nulls().cloned(),
        )) as ArrayRef;
        &binary
    } else {
        input
    };
    super::decode::decode(input, super::charset::Charset::Utf8, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn long_valid_runs_around_malformed_bytes_keep_jdk_replacement_groups() {
        for length in [0, 1, 31, 32, 33, 63, 64, 65, 4096] {
            for (invalid, replacement) in [
                (&b"\xed\xa0\x80"[..], "\u{fffd}"),
                (&b"\xed\xa0"[..], "\u{fffd}"),
                (&b"\xf0\x90\x80"[..], "\u{fffd}"),
                (&b"\xe0\x80\x80"[..], "\u{fffd}\u{fffd}\u{fffd}"),
            ] {
                let prefix = "a".repeat(length) + "\u{1f600}\u{4e2d}";
                let mut bytes = prefix.as_bytes().to_vec();
                bytes.extend(invalid);
                // ASCII terminates an incomplete malformed unit without changing its grouping.
                let suffix = "tail\0".to_owned() + &"z".repeat(length) + "\u{4e2d}\u{1f600}";
                bytes.extend(suffix.as_bytes());
                let input: ArrayRef = Arc::new(BinaryArray::from_iter_values([bytes.as_slice()]));
                let result = cast(&[input]).unwrap();
                let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(strings.value(0), prefix + replacement + &suffix);
            }
        }
    }

    #[test]
    fn fixed_slices_share_valid_utf8_bytes_offsets_and_nulls() {
        let input: ArrayRef = Arc::new(BinaryArray::from_iter([
            Some(&b"skip"[..]),
            None,
            Some(&b"a\0\0\0"[..]),
            Some(&b"text"[..]),
        ]));
        let fixed = arrow::compute::cast(&input, &DataType::FixedSizeBinary(4)).unwrap();
        let sliced = fixed.slice(1, 3);
        let output = cast(&[sliced.clone()]).unwrap();
        let strings = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings,
            &StringArray::from(vec![None, Some("a\0\0\0"), Some("text")])
        );
        let fixed = sliced
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(strings.values().as_ptr(), fixed.values().as_ptr());
        assert_eq!(strings.nulls(), fixed.nulls());
        output.to_data().validate_full().unwrap();
        assert_eq!(cast(&[sliced.slice(0, 0)]).unwrap().len(), 0);
    }

    #[test]
    fn malformed_fixed_bytes_use_jdk_grouping_and_keep_sliced_validity() {
        let input: ArrayRef = Arc::new(BinaryArray::from_iter([
            Some(&b"skip"[..]),
            Some(&b"\xed\xa0\x80\0"[..]),
            None,
            Some(&b"\xf0\x90\x80\0"[..]),
            Some(&b"\xf0\x9f\x98\x80"[..]),
        ]));
        for input in [
            input.clone(),
            arrow::compute::cast(&input, &DataType::FixedSizeBinary(4)).unwrap(),
        ] {
            let output = cast(&[input.slice(1, 4)]).unwrap();
            assert_eq!(
                output.as_any().downcast_ref::<StringArray>().unwrap(),
                &StringArray::from(vec![
                    Some("\u{fffd}\0"),
                    None,
                    Some("\u{fffd}\0"),
                    Some("\u{1f600}")
                ])
            );
            output.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn modern_bounded_targets_cut_utf16_units_and_pad_char() {
        let input: ArrayRef = Arc::new(BinaryArray::from_iter([
            Some(&b"skip"[..]),
            Some("a\u{1f600}b".as_bytes()),
            None,
            Some(&b"\xed\xa0\x80z"[..]),
            Some(&b""[..]),
            Some("\u{4e2d}".as_bytes()),
        ]));
        let input = input.slice(1, 5);
        for (length, expected) in [
            (1, ["a", "\u{fffd}", "", "\u{4e2d}"]),
            (2, ["a?", "\u{fffd}z", "", "\u{4e2d}"]),
            (3, ["a\u{1f600}", "\u{fffd}z", "", "\u{4e2d}"]),
            (-2, ["a?", "\u{fffd}z", "  ", "\u{4e2d} "]),
            (
                -5,
                ["a\u{1f600}b ", "\u{fffd}z   ", "     ", "\u{4e2d}    "],
            ),
        ] {
            let output = make_bounded(Some(length))(&[input.clone()]).unwrap();
            let mut expected = expected.map(Some).to_vec();
            expected.insert(1, None);
            assert_eq!(
                output.as_any().downcast_ref::<StringArray>().unwrap(),
                &StringArray::from(expected),
                "{length}"
            );
            output.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn bounded_rows_that_already_fit_share_decoded_buffers() {
        let input: ArrayRef = Arc::new(BinaryArray::from_iter([Some(&b"ab"[..]), None]));
        let fixed = arrow::compute::cast(&input, &DataType::FixedSizeBinary(2)).unwrap();
        for length in [2, 9, -2] {
            let output = make_bounded(Some(length))(&[fixed.clone()]).unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .values()
                    .as_ptr(),
                fixed
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .values()
                    .as_ptr()
            );
        }
        let invoke = |length| {
            function().invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(input.clone()),
                    ColumnarValue::Scalar(ScalarValue::Int32(length)),
                ],
                arg_fields: vec![],
                number_rows: 2,
                return_field: Arc::new(arrow::datatypes::Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
            })
        };
        assert!(invoke(None).is_err());
        assert!(invoke(Some(0)).is_err());
        assert!(invoke(Some(-1)).is_ok());
    }
}
