use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;
use std::fmt::Write;
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_json_quote",
        vec![DataType::Utf8],
        DataType::Utf8,
        json_quote,
    )
}

fn json_quote(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("JSON_QUOTE expects one argument");
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
        text.push('"');
        for ch in value.chars() {
            match ch {
                '"' => text.push_str("\\\""),
                '\\' => text.push_str("\\\\"),
                '/' => text.push_str("\\/"),
                '\u{8}' => text.push_str("\\b"),
                '\u{c}' => text.push_str("\\f"),
                '\n' => text.push_str("\\n"),
                '\r' => text.push_str("\\r"),
                '\t' => text.push_str("\\t"),
                ch if ch.is_ascii() => text.push(ch),
                ch => {
                    // Flink advances one UTF-16 unit after codePointAt, so a supplementary
                    // character emits its full code point and then its low surrogate.
                    let _ = write!(text, "\\u{:04x}", ch as u32);
                    if ch.len_utf16() == 2 {
                        let _ = write!(text, "\\u{:04x}", 0xdc00 + ((ch as u32 - 0x10000) & 0x3ff));
                    }
                }
            }
        }
        text.push('"');
        super::check_string_capacity(output.values_slice().len(), text.len())?;
        output.append_value(&text);
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn quote_matches_flink_utf16_iteration_and_raw_controls() {
        let output = json_quote(&[Arc::new(StringArray::from(vec![
            Some("/\u{e9}\u{1f600}\0"),
            None,
        ]))])
        .unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec![Some("\"\\/\\u00e9\\u1f600\\ude00\0\""), None])
        );
        assert!(json_quote(&[]).is_err());
    }
}
