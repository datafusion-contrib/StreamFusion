use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_json_unquote",
        vec![DataType::Utf8],
        DataType::Utf8,
        json_unquote,
    )
}

fn json_unquote(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("JSON_UNQUOTE expects one argument");
    };
    let input = as_string_array(input)?;
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    let mut text = String::new();
    for value in input.iter() {
        let Some(value) = value else {
            output.append_null();
            continue;
        };
        text.clear();
        if value.len() >= 2
            && value.starts_with('"')
            && value.ends_with('"')
            && unescape(&value[1..value.len() - 1], &mut text)?
        {
            output.append_value(&text);
        } else {
            output.append_value(value);
        }
    }
    Ok(Arc::new(output.finish()))
}

// Jackson validates the first token only. Validation stops at an unescaped interior quote,
// while Flink's unescaper still processes all the original text between the outer quotes.
fn unescape(value: &str, output: &mut String) -> Result<bool> {
    let mut chars = value.chars();
    let mut prefix_complete = false;
    let mut high_surrogate = None;
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            if !prefix_complete && ch < ' ' {
                return Ok(false);
            }
            prefix_complete |= ch == '"';
            if high_surrogate.take().is_some() {
                output.push('?');
            }
            output.push(ch);
            continue;
        }
        let unit = match chars.next() {
            Some('"') => b'"' as u16,
            Some('\\') => b'\\' as u16,
            Some('/') => b'/' as u16,
            Some('b') => 8,
            Some('f') => 12,
            Some('n') => 10,
            Some('r') => 13,
            Some('t') => 9,
            Some('u') => {
                if prefix_complete && chars.as_str().encode_utf16().take(4).count() < 4 {
                    return exec_err!(
                        "JSON_UNQUOTE truncated Unicode escape after the first JSON token"
                    );
                }
                let Some(unit) = unicode_escape(&mut chars, !prefix_complete) else {
                    return Ok(false);
                };
                unit
            }
            None if prefix_complete => b'\\' as u16,
            _ => return Ok(false),
        };
        append_utf16_unit(unit, &mut high_surrogate, output);
    }
    if high_surrogate.is_some() {
        output.push('?');
    }
    Ok(true)
}

fn unicode_escape(chars: &mut std::str::Chars<'_>, validate_json: bool) -> Option<u16> {
    let first = chars.next()?;
    let signed = matches!(first, '+' | '-');
    let digit = |ch: char| {
        if validate_json && !ch.is_ascii_hexdigit() {
            return None;
        }
        super::scalar::java_hex_digit(ch).map(u16::from)
    };
    let mut code = if signed && !validate_json {
        0
    } else {
        digit(first)?
    };
    for _ in 0..3 {
        code = code * 16 + digit(chars.next()?)?;
    }
    Some(if first == '-' {
        code.wrapping_neg()
    } else {
        code
    })
}

fn append_utf16_unit(unit: u16, pending: &mut Option<u16>, output: &mut String) {
    if let Some(high) = pending.take() {
        if (0xdc00..=0xdfff).contains(&unit) {
            let code = 0x10000 + ((u32::from(high) - 0xd800) << 10) + u32::from(unit) - 0xdc00;
            output.push(char::from_u32(code).unwrap_or('?'));
            return;
        }
        output.push('?');
    }
    if (0xd800..=0xdbff).contains(&unit) {
        *pending = Some(unit);
    } else {
        output.push(char::from_u32(u32::from(unit)).unwrap_or('?'));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn unquote_preserves_invalid_json_and_decodes_surrogates() {
        let input = StringArray::from(vec![
            "\"\\ud83d\\ude00\"",
            "\"\\ud800\"",
            "\"bad\\q\"",
            " \"ok\" ",
            "\"a\" \"b\"",
        ]);
        let output = json_unquote(&[Arc::new(input)]).unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec!["\u{1f600}", "?", "\"bad\\q\"", " \"ok\" ", "a\" \"b"])
        );
        assert!(json_unquote(&[]).is_err());
    }
    #[test]
    fn truncated_suffix_escape_fails_only_after_valid_prefix() {
        for value in ["\"a\" \\u1\"", "\"a\" \\uX\"", "\"a\" \\u\u{1f600}\""] {
            assert!(json_unquote(&[Arc::new(StringArray::from(vec![value]))])
                .unwrap_err()
                .to_string()
                .contains("truncated Unicode escape"));
        }
        let value = "\"\\u1\"";
        let output = json_unquote(&[Arc::new(StringArray::from(vec![value]))]).unwrap();
        assert_eq!(as_string_array(&output).unwrap().value(0), value);
    }
}
