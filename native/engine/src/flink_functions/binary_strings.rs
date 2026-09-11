use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use std::sync::Arc;

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
    use base64::Engine;

    let [arg] = args else {
        return datafusion::common::exec_err!("string encoding expects one argument");
    };
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
    let mut offsets = Vec::with_capacity(strings.len() + 1);
    let mut total = 0usize;
    offsets.push(0i32);
    for string in strings {
        let length = string.map_or(Some(0), |s| {
            if base64 {
                base64::encoded_len(s.len(), true)
            } else {
                s.len().checked_mul(2)
            }
        });
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
    for (row, string) in strings.iter().enumerate() {
        let Some(string) = string else { continue };
        let output = &mut values[offsets[row] as usize..offsets[row + 1] as usize];
        if base64 {
            base64::engine::general_purpose::STANDARD
                .encode_slice(string, output)
                .map_err(|e| datafusion::common::exec_datafusion_err!("TO_BASE64: {e}"))?;
        } else {
            for (&byte, pair) in string.iter().zip(output.chunks_exact_mut(2)) {
                pair[0] = super::HEX_DIGITS[(byte >> 4) as usize];
                pair[1] = super::HEX_DIGITS[(byte & 15) as usize];
            }
        }
    }
    Ok(Arc::new(StringArray::new(
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        arrow::buffer::Buffer::from_vec(values),
        strings.nulls().cloned(),
    )))
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
}
