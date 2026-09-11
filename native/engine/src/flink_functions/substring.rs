use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::{as_int32_array, as_string_array},
    exec_err, Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(crate) fn function(arity: usize) -> ScalarUDF {
    let mut types = vec![DataType::Int32; arity];
    if let Some(first) = types.first_mut() {
        *first = DataType::Utf8;
    }
    super::udf("flink_substring", types, DataType::Utf8, substring)
}

fn substring(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !(2..=3).contains(&args.len()) {
        return exec_err!("SUBSTRING expects two or three arguments");
    }
    let input = as_string_array(&args[0])?;
    let start = as_int32_array(&args[1])?;
    let length = args.get(2).map(|array| as_int32_array(array)).transpose()?;
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    for row in 0..input.len() {
        if input.is_null(row) || start.is_null(row) || length.is_some_and(|len| len.is_null(row)) {
            output.append_null();
            continue;
        }
        match slice(
            input.value(row),
            start.value(row),
            length.map_or(i32::MAX, |len| len.value(row)),
        ) {
            Some(value) => output.append_value(value),
            None => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

fn slice(value: &str, start: i32, length: i32) -> Option<&str> {
    if length < 0 {
        return None;
    }
    if value.is_ascii() {
        let begin = if start < 0 {
            value.len() as i64 + i64::from(start)
        } else {
            (i64::from(start) - 1).max(0)
        };
        if begin < 0 || begin as usize >= value.len() {
            return Some("");
        }
        let end = (begin as usize + length as usize).min(value.len());
        return Some(&value[begin as usize..end]);
    }
    let begin = if start < 0 {
        let Some(begin) = reverse_offset(value, start.unsigned_abs() as usize) else {
            return Some("");
        };
        begin
    } else {
        value
            .char_indices()
            .nth(start.saturating_sub(1).max(0) as usize)
            .map_or(value.len(), |(index, _)| index)
    };
    Some(prefix(&value[begin..], length as usize))
}

pub(super) fn reverse_offset(value: &str, count: usize) -> Option<usize> {
    if count == 0 {
        return Some(value.len());
    }
    value
        .char_indices()
        .rev()
        .nth(count - 1)
        .map(|(index, _)| index)
}

pub(super) fn prefix(value: &str, count: usize) -> &str {
    let end = value
        .char_indices()
        .nth(count)
        .map_or(value.len(), |(index, _)| index);
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn substring_handles_flink_start_and_length_bounds() {
        assert_eq!(slice("a\u{1f600}b", -2, 1), Some("\u{1f600}"));
        assert_eq!(slice("abc", -4, 5), Some(""));
        assert_eq!(slice("abc", 0, 2), Some("ab"));
        assert_eq!(slice("abc", i32::MIN, i32::MAX), Some(""));
        assert_eq!(slice("abc", i32::MAX, i32::MAX), Some(""));
        assert_eq!(slice("", 0, -1), None);
        assert!(substring(&[]).is_err());
    }
}
