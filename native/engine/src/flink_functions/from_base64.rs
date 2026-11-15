use arrow::array::{
    Array, ArrayAccessor, ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray,
};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(FromBase64 {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FromBase64 {
    signature: Signature,
}

impl ScalarUDFImpl for FromBase64 {
    fn name(&self) -> &str {
        "flink_from_base64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, inputs: &[DataType]) -> Result<DataType> {
        match inputs {
            [DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)] => {
                Ok(DataType::Binary)
            }
            _ => exec_err!("FROM_BASE64 expects one character or binary argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(decode, vec![])(&args.args)
    }
}

fn decode(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [arg] = args else {
        return exec_err!("FROM_BASE64 expects one argument");
    };
    if let Some(values) = arg.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return decode_bytes(values);
    }
    let binary;
    let values = if let Some(strings) = arg.as_any().downcast_ref::<StringArray>() {
        binary = BinaryArray::new(
            strings.offsets().clone(),
            strings.values().clone(),
            strings.nulls().cloned(),
        );
        &binary
    } else {
        datafusion::common::cast::as_binary_array(arg)?
    };
    decode_bytes(values)
}

fn decode_bytes<'a>(strings: impl ArrayAccessor<Item = &'a [u8]>) -> Result<ArrayRef> {
    use base64_simd::AsOut;

    let mut offsets = Vec::with_capacity(strings.len() + 1);
    let mut total = 0usize;
    offsets.push(0i32);
    for row in 0..strings.len() {
        if strings.is_valid(row) {
            let length = decoded_length(strings.value(row))?;
            total = total.checked_add(length).ok_or_else(|| {
                datafusion::common::exec_datafusion_err!(
                    "FROM_BASE64 output exceeds Binary capacity"
                )
            })?;
        }
        offsets.push(i32::try_from(total).map_err(|_| {
            datafusion::common::exec_datafusion_err!("FROM_BASE64 output exceeds Binary capacity")
        })?);
    }
    let mut values = vec![0u8; total];
    for row in 0..strings.len() {
        if strings.is_null(row) {
            continue;
        }
        let input = strings.value(row);
        let output = &mut values[offsets[row] as usize..offsets[row + 1] as usize];
        // SIMD validates complete groups. The JDK accepts unused nonzero tail bits, unlike
        // strict RFC 4648 decoders, so decode the last (possibly padded) group ourselves.
        let prefix = input.len().saturating_sub(1) / 4 * 4;
        let written = prefix / 4 * 3;
        if base64_simd::STANDARD_NO_PAD
            .decode(&input[..prefix], output[..written].as_out())
            .is_err()
        {
            return Err(invalid_input(input));
        }
        let remaining = output.len() - written;
        let count = if remaining == 0 { 0 } else { remaining + 1 };
        let tail = &input[prefix..prefix + count];
        let mut bits = 0u32;
        for &byte in tail {
            let digit = DIGITS[byte as usize];
            if digit == INVALID {
                return Err(invalid_input(input));
            }
            bits = (bits << 6) | u32::from(digit);
        }
        for (index, byte) in output[written..].iter_mut().enumerate() {
            *byte = (bits >> (count * 6 - 8 * (index + 1))) as u8;
        }
    }
    Ok(Arc::new(BinaryArray::new(
        OffsetBuffer::new(offsets.into()),
        Buffer::from_vec(values),
        strings.nulls().cloned(),
    )))
}

fn decoded_length(input: &[u8]) -> Result<usize> {
    let length = input.len();
    if length == 0 {
        return Ok(0);
    }
    if length < 2 {
        return Err(invalid_input(input));
    }
    let padding = if input[length - 1] == b'=' {
        if input[length - 2] == b'=' {
            2
        } else {
            1
        }
    } else {
        0
    };
    let digits = length - padding;
    let remainder = digits % 4;
    if remainder == 1 || padding != 0 && (length % 4 != 0 || padding + remainder != 4) {
        return Err(invalid_input(input));
    }
    Ok(digits / 4 * 3 + remainder * 6 / 8)
}

const INVALID: u8 = 0xff;
const DIGITS: [u8; 256] = {
    let mut table = [INVALID; 256];
    let mut index = 0;
    while index < 64 {
        table
            [b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"[index] as usize] =
            index as u8;
        index += 1;
    }
    table
};

// This cold path mirrors the released JDK basic decoder's validation, including signed-byte
// hexadecimal diagnostics. It never calls Java or publishes a partially decoded batch.
fn invalid_input(input: &[u8]) -> DataFusionError {
    let error = |message| {
        DataFusionError::External(Box::new(
            streamfusion_bridge::FlinkException::illegal_argument(message),
        ))
    };
    if input.len() < 2 {
        return error("Input byte[] should at least have 2 bytes for base64 bytes".into());
    }
    let mut shift = 18;
    let mut position = 0;
    while position < input.len() {
        let byte = input[position];
        position += 1;
        if byte == b'=' {
            if shift == 18 || shift == 6 && (position == input.len() || input[position] != b'=') {
                return error("Input byte array has wrong 4-byte ending unit".into());
            }
            if shift == 6 {
                position += 1;
            }
            break;
        }
        if DIGITS[byte as usize] == INVALID {
            let signed = i32::from(byte as i8);
            let hex = if signed < 0 {
                format!("-{:x}", -signed)
            } else {
                format!("{signed:x}")
            };
            return error(format!("Illegal base64 character {hex}"));
        }
        shift -= 6;
        if shift < 0 {
            shift = 18;
        }
    }
    if shift == 12 {
        return error("Last unit does not have enough valid bits".into());
    }
    error(format!(
        "Input byte array has incorrect ending byte at {position}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn jdk_tails_accept_missing_padding_and_unused_nonzero_bits() {
        let input = StringArray::from(vec![
            Some("!outside-the-slice"),
            Some(""),
            None,
            Some("YQ=="),
            Some("YR=="),
            Some("YR"),
            Some("YWI="),
            Some("YWL="),
            Some("YWL"),
            Some("YWJj"),
            Some("/w=="),
            Some("7aCA"),
            Some("AA=="),
        ])
        .slice(1, 12);
        let output = decode(&[Arc::new(input)]).unwrap();
        let output = datafusion::common::cast::as_binary_array(&output).unwrap();
        assert_eq!(
            output,
            &BinaryArray::from(vec![
                Some(&b""[..]),
                None,
                Some(&b"a"[..]),
                Some(&b"a"[..]),
                Some(&b"a"[..]),
                Some(&b"ab"[..]),
                Some(&b"ab"[..]),
                Some(&b"ab"[..]),
                Some(&b"abc"[..]),
                Some(&b"\xff"[..]),
                Some(&b"\xed\xa0\x80"[..]),
                Some(&b"\0"[..]),
            ])
        );
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn vector_boundaries_fixed_binary_and_all_byte_values_round_trip() {
        for length in [
            0, 1, 2, 3, 4, 15, 16, 17, 31, 32, 33, 63, 64, 65, 255, 256, 257, 4097,
        ] {
            let bytes: Vec<u8> = (0..length).map(|i| i as u8).collect();
            let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
            for input in [
                Arc::new(BinaryArray::from(vec![Some(encoded.as_bytes()), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some(encoded.as_str()), None])),
            ] {
                let output = decode(&[input]).unwrap();
                let output = datafusion::common::cast::as_binary_array(&output).unwrap();
                assert_eq!(output.value(0), bytes);
                assert!(output.is_null(1));
                output.to_data().validate_full().unwrap();
            }
            if !encoded.is_empty() {
                let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(encoded.as_bytes()), None].into_iter(),
                    encoded.len() as i32,
                )
                .unwrap();
                let output = decode(&[Arc::new(fixed)]).unwrap();
                assert_eq!(
                    datafusion::common::cast::as_binary_array(&output)
                        .unwrap()
                        .value(0),
                    bytes
                );
            }
        }
        let empty = decode(&[Arc::new(StringArray::from(Vec::<&str>::new()))]).unwrap();
        assert!(empty.is_empty());
        empty.to_data().validate_full().unwrap();
    }

    #[test]
    fn invalid_basic_encoding_retains_jdk_diagnostics() {
        for (input, message) in [
            (
                &b"A"[..],
                "Input byte[] should at least have 2 bytes for base64 bytes",
            ),
            (&b"!A"[..], "Illegal base64 character 21"),
            (&b"\xffA"[..], "Illegal base64 character -1"),
            (&b"Y WJj"[..], "Illegal base64 character 20"),
            (&b"YQ-_"[..], "Illegal base64 character 2d"),
            (&b"YQ="[..], "Input byte array has wrong 4-byte ending unit"),
            (
                &b"===="[..],
                "Input byte array has wrong 4-byte ending unit",
            ),
            (
                &b"AAAA=AAA"[..],
                "Input byte array has wrong 4-byte ending unit",
            ),
            (
                &b"=AAA"[..],
                "Input byte array has wrong 4-byte ending unit",
            ),
            (&b"A="[..], "Last unit does not have enough valid bits"),
            (&b"AAAAA"[..], "Last unit does not have enough valid bits"),
            (
                &b"YQ==="[..],
                "Input byte array has incorrect ending byte at 4",
            ),
            (
                &b"YQ==Z"[..],
                "Input byte array has incorrect ending byte at 4",
            ),
            (
                &b"YWI=\n"[..],
                "Input byte array has incorrect ending byte at 4",
            ),
        ] {
            let error = decode(&[Arc::new(BinaryArray::from(vec![input]))]).unwrap_err();
            let DataFusionError::External(error) = error else {
                panic!("{error}")
            };
            assert!(error.is::<streamfusion_bridge::FlinkException>());
            assert_eq!(error.to_string(), message, "{input:?}");
        }
    }

    #[test]
    fn scalar_nulls_stay_binary_and_null_payloads_are_not_decoded() {
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::Field;
        use datafusion::common::ScalarValue;

        for input in [ScalarValue::Utf8(None), ScalarValue::Binary(None)] {
            let output = function()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Scalar(input)],
                    arg_fields: vec![],
                    number_rows: 1,
                    return_field: Arc::new(Field::new("out", DataType::Binary, true)),
                    config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
                })
                .unwrap();
            assert!(matches!(
                output,
                ColumnarValue::Scalar(ScalarValue::Binary(None))
            ));
        }
        let input = BinaryArray::new(
            OffsetBuffer::new(vec![0i32, 4].into()),
            Buffer::from_vec(b"!bad".to_vec()),
            Some(NullBuffer::new_null(1)),
        );
        let output = decode(&[Arc::new(input)]).unwrap();
        assert!(output.is_null(0));
        output.to_data().validate_full().unwrap();
    }
}
