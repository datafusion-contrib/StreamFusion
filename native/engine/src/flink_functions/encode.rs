use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use datafusion::common::{cast::as_string_array, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

use super::charset::Charset;

pub(crate) fn function() -> ScalarUDF {
    super::charset::function(false)
}

pub(super) fn encode(input: &ArrayRef, charset: Charset) -> Result<ArrayRef> {
    let input = as_string_array(input)?;
    if matches!(charset, Charset::Utf8) {
        return Ok(Arc::new(BinaryArray::new(
            input.offsets().clone(),
            input.values().clone(),
            input.nulls().cloned(),
        )));
    }
    if matches!(
        charset,
        Charset::Utf16 | Charset::Utf16Be | Charset::Utf16Le
    ) {
        return encode_utf16(input, charset);
    }
    let limit = if matches!(charset, Charset::Ascii) {
        127
    } else {
        255
    };
    let mut output = BinaryBuilder::with_capacity(input.len(), input.values().len());
    let mut bytes = Vec::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        bytes.clear();
        bytes.extend(
            input
                .value(row)
                .chars()
                .map(|c| if c as u32 <= limit { c as u8 } else { b'?' }),
        );
        output.append_value(&bytes);
    }
    Ok(Arc::new(output.finish()))
}

fn encode_utf16(input: &arrow::array::StringArray, charset: Charset) -> Result<ArrayRef> {
    use std::io::Write;

    let mut output = BinaryBuilder::with_capacity(input.len(), input.values().len());
    for value in input {
        let Some(value) = value else {
            output.append_null();
            continue;
        };
        // The JDK writes a big-endian BOM only for non-empty UTF-16 strings.
        if matches!(charset, Charset::Utf16) && !value.is_empty() {
            output.write_all(&[0xfe, 0xff])?;
        }
        for unit in value.encode_utf16() {
            let bytes = if matches!(charset, Charset::Utf16Le) {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            output.write_all(&bytes)?;
        }
        super::check_string_capacity(output.values_slice().len(), 0)?;
        output.append_value([]);
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn encode_keeps_slices_nulls_and_jdk_replacement() {
        let input = StringArray::from(vec![
            Some("unused"),
            Some("a\u{e9}\u{1f600}"),
            None,
            Some(""),
        ])
        .slice(1, 3);
        for (charset, expected) in [
            (Charset::Utf8, "a\u{e9}\u{1f600}".as_bytes()),
            (Charset::Latin1, b"a\xe9?"),
            (Charset::Ascii, b"a??"),
        ] {
            let input: ArrayRef = Arc::new(input.clone());
            let output = encode(&input, charset).unwrap();
            let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(output.value(0), expected);
            assert!(output.is_null(1));
            assert_eq!(output.value(2), b"");
        }
    }

    #[test]
    fn utf16_byte_order_marks_supplementary_characters_and_empty_rows() {
        let input: ArrayRef = Arc::new(
            StringArray::from(vec![Some("skip"), Some("a\u{1f600}\0"), None, Some("")]).slice(1, 3),
        );
        for (charset, expected) in [
            (
                Charset::Utf16,
                &b"\xfe\xff\x00a\xd8\x3d\xde\x00\x00\x00"[..],
            ),
            (Charset::Utf16Be, &b"\x00a\xd8\x3d\xde\x00\x00\x00"[..]),
            (Charset::Utf16Le, &b"a\x00\x3d\xd8\x00\xde\x00\x00"[..]),
        ] {
            let result = encode(&input, charset).unwrap();
            let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(result.value(0), expected);
            assert!(result.is_null(1));
            assert_eq!(result.value(2), b"");
            result.to_data().validate_full().unwrap();
        }
    }
}
