use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::common::{
    cast::{as_binary_array, as_string_array},
    Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

use super::charset::Charset;

pub(crate) fn function() -> ScalarUDF {
    super::charset::function(true)
}

pub(super) fn decode(
    input: &ArrayRef,
    charset: Charset,
    replace_reversed_bom: bool,
) -> Result<ArrayRef> {
    let input = as_binary_array(input)?;
    if matches!(charset, Charset::Utf8) {
        if let Ok(strings) = StringArray::try_new(
            input.offsets().clone(),
            input.values().clone(),
            input.nulls().cloned(),
        ) {
            return Ok(Arc::new(strings));
        }
    }
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    let mut text = String::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        text.clear();
        match charset {
            Charset::Utf16 | Charset::Utf16Be | Charset::Utf16Le => {
                append_utf16(input.value(row), charset, replace_reversed_bom, &mut text);
            }
            Charset::Utf8 => super::scalar::append_java_utf8(input.value(row), &mut text),
            Charset::Latin1 => text.extend(input.value(row).iter().map(|&byte| char::from(byte))),
            Charset::Ascii => text.extend(input.value(row).iter().map(|&byte| {
                if byte < 128 {
                    char::from(byte)
                } else {
                    '\u{fffd}'
                }
            })),
        }
        super::check_string_capacity(output.values_slice().len(), text.len())?;
        output.append_value(&text);
    }
    Ok(Arc::new(output.finish()))
}

fn append_utf16(
    mut bytes: &[u8],
    charset: Charset,
    replace_reversed_bom: bool,
    output: &mut String,
) {
    let little_endian = match charset {
        Charset::Utf16 if bytes.starts_with(&[0xff, 0xfe]) => {
            bytes = &bytes[2..];
            true
        }
        Charset::Utf16 if bytes.starts_with(&[0xfe, 0xff]) => {
            bytes = &bytes[2..];
            false
        }
        _ => matches!(charset, Charset::Utf16Le),
    };
    let unit = |bytes: &[u8]| {
        if little_endian {
            u16::from_le_bytes([bytes[0], bytes[1]])
        } else {
            u16::from_be_bytes([bytes[0], bytes[1]])
        }
    };
    while bytes.len() >= 2 {
        let first = unit(bytes);
        bytes = &bytes[2..];
        if (0xd800..=0xdbff).contains(&first) {
            if bytes.len() < 2 {
                // JDK underflow replaces the high surrogate and a trailing odd byte together.
                output.push('\u{fffd}');
                return;
            }
            let second = unit(bytes);
            bytes = &bytes[2..];
            // UnicodeDecoder consumes both units even when the second is not a low surrogate.
            let character = if (0xdc00..=0xdfff).contains(&second) {
                char::from_u32(0x10000 + ((first as u32 - 0xd800) << 10) + second as u32 - 0xdc00)
            } else {
                None
            };
            output.push(character.unwrap_or('\u{fffd}'));
        } else if replace_reversed_bom && first == 0xfffe {
            output.push('\u{fffd}');
        } else {
            output.push(char::from_u32(first as u32).unwrap_or('\u{fffd}'));
        }
    }
    if !bytes.is_empty() {
        output.push('\u{fffd}');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};

    #[test]
    fn utf16_runtime_profiles_preserve_bom_and_error_consumption() {
        for (charset, bytes, modern, legacy) in [
            (Charset::Utf16, &b"\xff\xfe"[..], "", ""),
            (
                Charset::Utf16,
                &b"\xfe\xff\xff\xfe"[..],
                "\u{fffe}",
                "\u{fffd}",
            ),
            (
                Charset::Utf16,
                &b"\xff\xfe\xfe\xff"[..],
                "\u{fffe}",
                "\u{fffd}",
            ),
            (
                Charset::Utf16Be,
                &b"\xff\xfe\xfe\xff"[..],
                "\u{fffe}\u{feff}",
                "\u{fffd}\u{feff}",
            ),
            (
                Charset::Utf16Le,
                &b"\xfe\xff\xff\xfe"[..],
                "\u{fffe}\u{feff}",
                "\u{fffd}\u{feff}",
            ),
            (
                Charset::Utf16Be,
                &b"\x00a\xff\xfe\x00"[..],
                "a\u{fffe}\u{fffd}",
                "a\u{fffd}\u{fffd}",
            ),
            (
                Charset::Utf16Be,
                &b"\xd8\x00\xff\xfe"[..],
                "\u{fffd}",
                "\u{fffd}",
            ),
            (
                Charset::Utf16Be,
                &b"\xdc\x00\xff\xfe"[..],
                "\u{fffd}\u{fffe}",
                "\u{fffd}\u{fffd}",
            ),
            (
                Charset::Utf16Be,
                &b"\xd8\x3d\xde\x00\xff\xfe"[..],
                "\u{1f600}\u{fffe}",
                "\u{1f600}\u{fffd}",
            ),
        ] {
            let input: ArrayRef = Arc::new(
                BinaryArray::from(vec![Some(&b"skip"[..]), None, Some(bytes), Some(&b""[..])])
                    .slice(1, 3),
            );
            for (replace, expected) in [(false, modern), (true, legacy)] {
                let result = decode(&input, charset, replace).unwrap();
                assert_eq!(
                    as_string_array(&result).unwrap(),
                    &StringArray::from(vec![None, Some(expected), Some("")])
                );
                result.to_data().validate_full().unwrap();
                assert_eq!(
                    decode(&input.slice(0, 0), charset, replace).unwrap().len(),
                    0
                );
            }
        }
    }

    #[test]
    fn decode_profile_is_a_non_null_literal_and_applies_to_scalars() {
        let invoke = |profile: Option<ColumnarValue>| {
            let mut args = vec![
                ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![0xff, 0xfe]))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTF-16BE".into()))),
            ];
            args.extend(profile);
            function().invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
            })
        };
        for (profile, expected) in [
            (None, "\u{fffe}"),
            (Some(false), "\u{fffe}"),
            (Some(true), "\u{fffd}"),
        ] {
            let result =
                invoke(profile.map(|flag| ColumnarValue::Scalar(ScalarValue::Boolean(Some(flag)))))
                    .unwrap();
            assert!(
                matches!(result, ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) if text == expected)
            );
        }
        assert!(invoke(Some(ColumnarValue::Scalar(ScalarValue::Boolean(None)))).is_err());
        assert!(invoke(Some(ColumnarValue::Array(Arc::new(
            arrow::array::BooleanArray::from(vec![true])
        ))))
        .is_err());
    }

    #[test]
    fn decode_keeps_jdk_malformed_sequence_grouping() {
        let input: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&b"\xed\xa0\x80"[..]),
            Some(&b"\xf0\x90\x80"[..]),
            None,
        ]));
        let output = decode(&input, Charset::Utf8, false).unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec![Some("\u{fffd}"), Some("\u{fffd}"), None])
        );
    }
    #[test]
    fn valid_utf8_reuses_values_offsets_and_validity_on_a_slice() {
        let input = BinaryArray::from(vec![
            Some(&b"skip"[..]),
            Some("a\u{1f600}\0".as_bytes()),
            None,
            Some(&b""[..]),
        ])
        .slice(1, 3);
        let values = input.values().as_ptr();
        let offsets = input.offsets().as_ptr();
        let input: ArrayRef = Arc::new(input);
        let output = decode(&input, Charset::Utf8, false).unwrap();
        let strings = as_string_array(&output).unwrap();
        assert_eq!(
            strings,
            &StringArray::from(vec![Some("a\u{1f600}\0"), None, Some("")])
        );
        assert_eq!(strings.values().as_ptr(), values);
        assert_eq!(strings.offsets().as_ptr(), offsets);
        strings.to_data().validate_full().unwrap();
    }

    #[test]
    fn utf16_matches_jdk_bom_and_malformed_grouping() {
        for (bytes, expected) in [
            (&b"\xfe\xff\x00a"[..], "a"),
            (&b"\xff\xfea\x00"[..], "a"),
            (&b"\x00a\xfe\xff\xff\xfe"[..], "a\u{feff}\u{fffe}"),
            (&b"\xd8\x3d\xde\x00"[..], "\u{1f600}"),
            (&b"\xd8\x00\x00a\x00b"[..], "\u{fffd}b"),
            (&b"\xd8\x00\x00"[..], "\u{fffd}"),
            (&b"\xdc\x00\x00a\x00"[..], "\u{fffd}a\u{fffd}"),
            (&b"\xfe"[..], "\u{fffd}"),
            (&b"\xfe\xff"[..], ""),
        ] {
            let mut output = String::new();
            append_utf16(bytes, Charset::Utf16, false, &mut output);
            assert_eq!(output, expected, "{bytes:02x?}");
        }
        for (charset, bytes) in [
            (Charset::Utf16Be, &b"\xfe\xff\xd8\x00\x00a"[..]),
            (Charset::Utf16Le, &b"\xff\xfe\x00\xd8a\x00"[..]),
        ] {
            let input: ArrayRef = Arc::new(
                BinaryArray::from(vec![Some(&b"skip"[..]), None, Some(bytes), Some(&b""[..])])
                    .slice(1, 3),
            );
            let result = decode(&input, charset, false).unwrap();
            assert_eq!(
                as_string_array(&result).unwrap(),
                &StringArray::from(vec![None, Some("\u{feff}\u{fffd}"), Some("")])
            );
            result.to_data().validate_full().unwrap();
        }
    }
}
