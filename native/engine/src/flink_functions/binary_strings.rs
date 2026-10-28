use arrow::array::{
    Array, ArrayAccessor, ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray,
};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(super) fn base64_binary() -> ScalarUDF {
    ScalarUDF::new_from_impl(Base64Binary {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Base64Binary {
    signature: Signature,
}

impl ScalarUDFImpl for Base64Binary {
    fn name(&self) -> &str {
        "flink_to_base64_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, inputs: &[DataType]) -> datafusion::common::Result<DataType> {
        match inputs {
            [DataType::Binary | DataType::FixedSizeBinary(_)] => Ok(DataType::Utf8),
            _ => datafusion::common::exec_err!("TO_BASE64 expects one binary argument"),
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(|arrays| encode(arrays, true), vec![])(
            &args.args,
        )
    }
}

pub(super) fn hex_int(args: &[ArrayRef]) -> datafusion::common::Result<ArrayRef> {
    let [arg] = args else {
        return datafusion::common::exec_err!("HEX expects one integer argument");
    };
    let values = datafusion::common::cast::as_int64_array(arg)?;
    let mut output = arrow::array::StringBuilder::with_capacity(values.len(), values.len() * 16);
    for value in values {
        match value {
            Some(value) => {
                // Like Comet's integer HEX, keep at most 16 digits on the stack.
                let mut digits = [0u8; 16];
                let mut start = digits.len();
                let mut remaining = value as u64;
                loop {
                    start -= 1;
                    digits[start] = super::HEX_DIGITS[(remaining & 15) as usize];
                    remaining >>= 4;
                    if remaining == 0 {
                        break;
                    }
                }
                // SAFETY: the written suffix contains only ASCII bytes from HEX_DIGITS.
                output.append_value(unsafe { std::str::from_utf8_unchecked(&digits[start..]) });
            }
            None => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

pub(super) fn bin(args: &[ArrayRef]) -> datafusion::common::Result<ArrayRef> {
    use std::fmt::Write;

    let [arg] = args else {
        return datafusion::common::exec_err!("BIN expects one argument");
    };
    let values = datafusion::common::cast::as_int64_array(arg)?;
    let mut builder = arrow::array::StringBuilder::with_capacity(values.len(), values.len() * 64);
    for value in values {
        match value {
            Some(value) => {
                // Flink uses Long.toBinaryString, including 64-bit two's complement negatives.
                write!(&mut builder, "{:b}", value as u64)
                    .map_err(|e| datafusion::common::exec_datafusion_err!("BIN: {e}"))?;
                builder.append_value("");
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn encode(args: &[ArrayRef], base64: bool) -> datafusion::common::Result<ArrayRef> {
    let [arg] = args else {
        return datafusion::common::exec_err!("string encoding expects one argument");
    };
    if let Some(values) = arg.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return encode_bytes(values, base64);
    }
    let binary;
    let strings = if let Some(strings) = arg.as_any().downcast_ref::<StringArray>() {
        binary = BinaryArray::new(
            strings.offsets().clone(),
            strings.values().clone(),
            strings.nulls().cloned(),
        );
        &binary
    } else {
        datafusion::common::cast::as_binary_array(arg)?
    };
    encode_bytes(strings, base64)
}

fn encode_bytes<'a>(
    strings: impl ArrayAccessor<Item = &'a [u8]>,
    base64: bool,
) -> datafusion::common::Result<ArrayRef> {
    use base64_simd::AsOut;

    let mut offsets = Vec::with_capacity(strings.len() + 1);
    let mut total = 0usize;
    offsets.push(0i32);
    for row in 0..strings.len() {
        let length = if strings.is_null(row) {
            Some(0)
        } else {
            let s = strings.value(row);
            if base64 {
                base64::encoded_len(s.len(), true)
            } else {
                s.len().checked_mul(2)
            }
        };
        total = length
            .and_then(|len| total.checked_add(len))
            .ok_or_else(|| {
                datafusion::common::exec_datafusion_err!("encoded string array exceeds capacity")
            })?;
        offsets.push(i32::try_from(total).map_err(|_| {
            datafusion::common::exec_datafusion_err!("encoded string array exceeds Utf8 capacity")
        })?);
    }
    // Sizes are known from byte lengths: write directly into the final Arrow values buffer.
    let mut values = vec![0u8; total];
    for row in 0..strings.len() {
        if strings.is_null(row) {
            continue;
        }
        let string = strings.value(row);
        let output = &mut values[offsets[row] as usize..offsets[row + 1] as usize];
        if base64 {
            let _ = base64_simd::STANDARD.encode(string, output.as_out());
        } else {
            for (&byte, pair) in string.iter().zip(output.chunks_exact_mut(2)) {
                pair[0] = super::HEX_DIGITS[(byte >> 4) as usize];
                pair[1] = super::HEX_DIGITS[(byte & 15) as usize];
            }
        }
    }
    let offsets = arrow::buffer::OffsetBuffer::new(offsets.into());
    let values = arrow::buffer::Buffer::from_vec(values);
    let nulls = strings.nulls().cloned();
    let output = if base64 {
        // SAFETY: checked offsets span the initialized buffer; the standard encoder writes only
        // ASCII into each exact-size range, and input validity has the same row count.
        unsafe { StringArray::new_unchecked(offsets, values, nulls) }
    } else {
        StringArray::new(offsets, values, nulls)
    };
    Ok(Arc::new(output))
}

pub(super) fn unhex(args: &[ArrayRef]) -> datafusion::common::Result<ArrayRef> {
    let [arg] = args else {
        return datafusion::common::exec_err!("UNHEX expects one argument");
    };
    let strings = datafusion::common::cast::as_string_array(arg)?;
    let input_offsets = strings.value_offsets();
    let byte_span = (input_offsets[strings.len()] - input_offsets[0]) as usize;
    // Each odd-length row needs one extra nibble. Use the slice's span, not its parent buffer.
    let mut values = Vec::with_capacity(byte_span.div_ceil(2) + strings.len() / 2);
    let mut offsets = Vec::with_capacity(strings.len() + 1);
    let mut nulls = arrow::array::NullBufferBuilder::new(strings.len());
    offsets.push(0i32);
    for string in strings {
        let row_start = values.len();
        let valid = if let Some(string) = string {
            values.resize(row_start + string.len().div_ceil(2), 0);
            decode_unhex(string.as_bytes(), &mut values[row_start..])
        } else {
            false
        };
        if !valid {
            values.truncate(row_start);
        }
        nulls.append(valid);
        offsets.push(i32::try_from(values.len()).map_err(|_| {
            datafusion::common::exec_datafusion_err!("UNHEX output exceeds Binary capacity")
        })?);
    }
    Ok(Arc::new(arrow::array::BinaryArray::new(
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        arrow::buffer::Buffer::from_vec(values),
        nulls.finish(),
    )))
}

fn decode_unhex(bytes: &[u8], output: &mut [u8]) -> bool {
    const INVALID: u8 = 0xff;
    const NIBBLES: [u8; 256] = {
        let mut table = [INVALID; 256];
        let mut i = 0;
        while i < table.len() {
            table[i] = match i as u8 {
                b'0'..=b'9' => i as u8 - b'0',
                b'A'..=b'F' => i as u8 - b'A' + 10,
                b'a'..=b'f' => i as u8 - b'a' + 10,
                _ => INVALID,
            };
            i += 1;
        }
        table
    };
    let odd = bytes.len() % 2;
    if odd == 1 {
        if NIBBLES[bytes[0] as usize] == INVALID {
            return false;
        }
        // Flink validates but discards the leading odd digit, leaving a zero output byte.
        output[0] = 0;
    }
    for (pair, out) in bytes[odd..].chunks_exact(2).zip(&mut output[odd..]) {
        let first = NIBBLES[pair[0] as usize];
        let second = NIBBLES[pair[1] as usize];
        // Valid nibbles use only four bits; either invalid digit makes the OR 0xff.
        if (first | second) == INVALID {
            return false;
        }
        *out = (first << 4) | second;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_preserves_arbitrary_bytes_on_a_nullable_slice() {
        let input = BinaryArray::from(vec![
            Some(&b"skip"[..]),
            Some(&b"\x00\xff\x80"[..]),
            None,
            Some(&b""[..]),
            Some(&b"a"[..]),
            Some(&b"ab"[..]),
        ])
        .slice(1, 5);
        let result = encode(&[Arc::new(input)], true).unwrap();
        let result = datafusion::common::cast::as_string_array(&result).unwrap();
        assert_eq!(
            result,
            &StringArray::from(vec![
                Some("AP+A"),
                None,
                Some(""),
                Some("YQ=="),
                Some("YWI=")
            ])
        );
        result.to_data().validate_full().unwrap();
    }

    #[test]
    fn fixed_base64_preserves_slices_nulls_padding_and_empty_batches() {
        for width in [1, 2, 3, 16, 256] {
            let bytes: Vec<u8> = (0..width).map(|i| (i * 127) as u8).collect();
            let input = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(bytes.as_slice()), None, Some(bytes.as_slice())].into_iter(),
                width,
            )
            .unwrap();
            let binary = arrow::compute::cast(&input, &DataType::Binary).unwrap();
            for (offset, length) in [(0, 3), (1, 2), (2, 1), (1, 0)] {
                let fixed: ArrayRef = Arc::new(input.slice(offset, length));
                let variable = binary.slice(offset, length);
                let expected = encode(&[variable], true).unwrap();
                let actual = encode(&[fixed.clone()], true).unwrap();
                assert_eq!(actual.as_ref(), expected.as_ref());
                actual.to_data().validate_full().unwrap();
                let function = base64_binary();
                assert_eq!(
                    function.return_type(&[fixed.data_type().clone()]).unwrap(),
                    DataType::Utf8
                );
            }
        }
    }

    #[test]
    fn simd_base64_matches_the_reference_at_vector_and_padding_boundaries() {
        use base64::Engine;

        let mut values = vec![Some(b"skip".to_vec()), None];
        for width in (0..=260).chain([511, 512, 513, 4097, 65536]) {
            values.push(Some((0..width).map(|i| (i * 127 + width) as u8).collect()));
        }
        let expected = StringArray::from_iter(values[1..].iter().map(|value| {
            value
                .as_ref()
                .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes))
        }));
        let input = BinaryArray::from_iter(values.iter().map(|value| value.as_deref()))
            .slice(1, values.len() - 1);
        let actual = encode(&[Arc::new(input)], true).unwrap();
        assert_eq!(actual.as_ref(), &expected);
        actual.to_data().validate_full().unwrap();
    }
}
