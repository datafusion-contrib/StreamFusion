use std::fmt::{self, Write};

/// Jackson string escaping. Unescaped UTF-8 spans are copied together, as in Comet's JSON writer.
pub fn write_json_string(out: &mut impl Write, value: &str) -> fmt::Result {
    out.write_char('"')?;
    let mut start = 0;
    for (index, &byte) in value.as_bytes().iter().enumerate() {
        let escape = match byte {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\x08' => "\\b",
            b'\t' => "\\t",
            b'\n' => "\\n",
            b'\x0c' => "\\f",
            b'\r' => "\\r",
            0..=0x1f => "",
            _ => continue,
        };
        // Only ASCII bytes split a span, so these offsets are UTF-8 boundaries.
        out.write_str(&value[start..index])?;
        if escape.is_empty() {
            write!(out, "\\u00{:02X}", byte)?;
        } else {
            out.write_str(escape)?;
        }
        start = index + 1;
    }
    out.write_str(&value[start..])?;
    out.write_char('"')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jackson_escapes_controls_but_preserves_slashes_and_unicode() {
        let mut out = String::new();
        write_json_string(
            &mut out,
            "a/\"\\\0\u{1f}\x08\t\n\x0c\r\u{7f}\u{80}\u{1f600}",
        )
        .unwrap();
        assert_eq!(
            out,
            "\"a/\\\"\\\\\\u0000\\u001F\\b\\t\\n\\f\\r\u{7f}\u{80}\u{1f600}\""
        );
    }
}
