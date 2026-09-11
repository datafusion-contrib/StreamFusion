use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::{as_int32_array, as_string_array},
    exec_datafusion_err, exec_err, Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::fmt::Write;
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_lpad",
        vec![DataType::Utf8, DataType::Int32, DataType::Utf8],
        DataType::Utf8,
        |args| pad(args, true),
    )
}

pub(super) fn pad(args: &[ArrayRef], left: bool) -> Result<ArrayRef> {
    let [input, length, padding] = args else {
        return exec_err!("LPAD/RPAD expects three arguments");
    };
    let input = as_string_array(input)?;
    let length = as_int32_array(length)?;
    let padding = as_string_array(padding)?;
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    for row in 0..input.len() {
        if input.is_null(row)
            || length.is_null(row)
            || padding.is_null(row)
            || length.value(row) < 0
            || padding.value(row).is_empty()
        {
            output.append_null();
            continue;
        }
        let count = length.value(row) as usize;
        let base = Utf16Prefix::new(input.value(row), count);
        let pattern = padding.value(row);
        let pattern_units = if pattern.is_ascii() {
            pattern.len()
        } else {
            pattern.encode_utf16().count()
        };
        let pad_count = count - base.units;
        let repeats = pad_count / pattern_units;
        let tail = Utf16Prefix::new(pattern, pad_count % pattern_units);
        let size = pattern
            .len()
            .checked_mul(repeats)
            .and_then(|bytes| bytes.checked_add(base.byte_len()))
            .and_then(|bytes| bytes.checked_add(tail.byte_len()))
            .ok_or_else(|| exec_datafusion_err!("LPAD/RPAD output size overflow"))?;
        super::check_string_capacity(output.values_slice().len(), size)?;
        if !left {
            base.append_to(&mut output)?;
        }
        for _ in 0..repeats {
            output.write_str(pattern)?;
        }
        tail.append_to(&mut output)?;
        if left {
            base.append_to(&mut output)?;
        }
        output.append_value("");
    }
    Ok(Arc::new(output.finish()))
}

struct Utf16Prefix<'a> {
    text: &'a str,
    units: usize,
    split_surrogate: bool,
}

impl<'a> Utf16Prefix<'a> {
    fn new(value: &'a str, limit: usize) -> Self {
        if value.is_ascii() {
            let units = value.len().min(limit);
            return Self {
                text: &value[..units],
                units,
                split_surrogate: false,
            };
        }
        let mut units = 0;
        for (offset, ch) in value.char_indices() {
            if units == limit {
                return Self {
                    text: &value[..offset],
                    units,
                    split_surrogate: false,
                };
            }
            if units + ch.len_utf16() > limit {
                return Self {
                    text: &value[..offset],
                    units: limit,
                    split_surrogate: true,
                };
            }
            units += ch.len_utf16();
        }
        Self {
            text: value,
            units,
            split_surrogate: false,
        }
    }

    fn byte_len(&self) -> usize {
        self.text.len() + usize::from(self.split_surrogate)
    }

    fn append_to(&self, output: &mut StringBuilder) -> Result<()> {
        output.write_str(self.text)?;
        // A UTF-16 prefix cut can leave a high surrogate; JDK UTF-8 encodes it as '?'.
        // The next intact UTF-8 span cannot start with a lone low surrogate.
        if self.split_surrogate {
            output.write_char('?')?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    #[test]
    fn padding_cuts_utf16_surrogates_and_preserves_nulls_and_slices() {
        let input: ArrayRef = Arc::new(
            StringArray::from(vec![
                Some("skip"),
                Some("a\u{1f600}b"),
                Some("a"),
                Some("a"),
                Some("a"),
                None,
            ])
            .slice(1, 5),
        );
        let lengths: ArrayRef = Arc::new(Int32Array::from(vec![2, 4, -1, 1, 2]));
        let padding: ArrayRef = Arc::new(StringArray::from(vec!["x", "\u{1f600}", "x", "", "x"]));
        for (left, expected) in [(true, "\u{1f600}?a"), (false, "a\u{1f600}?")] {
            let output = pad(&[input.clone(), lengths.clone(), padding.clone()], left).unwrap();
            assert_eq!(
                as_string_array(&output).unwrap(),
                &StringArray::from(vec![Some("a?"), Some(expected), None, None, None])
            );
            output.to_data().validate_full().unwrap();
        }
        assert!(pad(&[], true).is_err());
    }
}
