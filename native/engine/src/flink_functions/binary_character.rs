use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(BinaryCharacter {
        signature: Signature::any(1, Volatility::Immutable),
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
            [DataType::Binary | DataType::FixedSizeBinary(_)] => Ok(DataType::Utf8),
            _ => exec_err!("Binary to STRING expects one binary argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(cast, vec![])(&args.args)
    }
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
}
