//! Batch certificates for builtins with a retained released-Flink evaluator.
//! A failed certificate never returns a partial batch or guesses at Java behavior.

use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

mod patterns;
mod printf;
mod trim;
mod url;

pub(super) fn integer_printf(pattern: &str, args: &[ArrayRef], rows: usize) -> Option<ArrayRef> {
    if !pattern.is_ascii() {
        return None;
    }
    printf::integer_literal(pattern, args, rows)
}

pub(crate) fn evaluate(
    operation: i64,
    arrays: &[ArrayRef],
    caches: &[ArrayRef],
    target: &DataType,
) -> Option<ArrayRef> {
    if operation == 12 {
        let [source, bits] = arrays else {
            return None;
        };
        return super::sha2_runtime::evaluate([source, bits]);
    }
    if operation == 11 {
        let [input, separator, index] = arrays else {
            return None;
        };
        return super::split_index::integer_separator([input, separator, index], caches);
    }
    for (index, input) in arrays.iter().enumerate() {
        if let Some(strings) = input.as_any().downcast_ref::<StringArray>() {
            let cache = caches
                .get(index)
                .filter(|a| a.data_type() != &DataType::Null)
                .map(JavaStringCacheColumn::new);
            for (row, value) in strings.iter().enumerate() {
                let Some(value) = value else { continue };
                if !value.is_ascii() {
                    return None;
                }
                if let Some(units) = cache.as_ref().and_then(|cache| cache.value(row)) {
                    if !ascii_cache_matches(value.as_bytes(), units) {
                        return None;
                    }
                }
            }
        }
    }
    match operation {
        1..=8 => patterns::evaluate(operation, arrays, target),
        9 => url::evaluate(arrays),
        10 => printf::evaluate(arrays),
        13..=15 => trim::evaluate(operation, arrays, caches),
        _ => None,
    }
}

fn ascii_cache_matches(bytes: &[u8], units: &[u16]) -> bool {
    if bytes.len() != units.len() {
        return false;
    }
    let mut offset = 0;
    // Widen eight ASCII bytes together so LLVM can compare UTF-16 lanes in parallel.
    while offset + 8 <= bytes.len() {
        let b = &bytes[offset..offset + 8];
        let widened = [
            u16::from(b[0]),
            u16::from(b[1]),
            u16::from(b[2]),
            u16::from(b[3]),
            u16::from(b[4]),
            u16::from(b[5]),
            u16::from(b[6]),
            u16::from(b[7]),
        ];
        if units[offset..offset + 8] != widened {
            return false;
        }
        offset += 8;
    }
    units[offset..]
        .iter()
        .copied()
        .eq(bytes[offset..].iter().copied().map(u16::from))
}

fn text(array: &ArrayRef, row: usize) -> Option<&str> {
    let values = array.as_any().downcast_ref::<StringArray>()?;
    (!values.is_null(row)).then(|| values.value(row))
}

fn integer(array: &ArrayRef, row: usize) -> Option<i64> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! primitive {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .map(|a| i64::from(a.value(row)))
        };
    }
    match array.data_type() {
        DataType::Int64 => primitive!(Int64Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int16 => primitive!(arrow::array::Int16Array),
        DataType::Int8 => primitive!(arrow::array::Int8Array),
        _ => None,
    }
}

fn number_output(values: Vec<Option<i64>>, target: &DataType) -> Option<ArrayRef> {
    Some(match target {
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as i32)),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values)),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(
            values.into_iter().map(|v| v.map(|v| v != 0)),
        )),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_java_utf16_must_agree_before_native_matching() {
        let args = [
            Arc::new(StringArray::from(vec!["?"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["."])) as ArrayRef,
        ];
        let cache = streamfusion_bridge::string_cache::string_cache_array([Some(&[0xd800][..])]);
        assert!(evaluate(1, &args, &[cache], &DataType::Boolean).is_none());
        assert!(evaluate(1, &args, &[], &DataType::Boolean).is_some());
    }

    #[test]
    fn ascii_cache_comparison_checks_every_lane_tail_and_length() {
        for length in 0..65 {
            let bytes: Vec<_> = (0..length).map(|i| (i * 31 % 128) as u8).collect();
            let mut units: Vec<_> = bytes.iter().copied().map(u16::from).collect();
            assert!(ascii_cache_matches(&bytes, &units));
            for index in 0..length {
                units[index] ^= 0xd800;
                assert!(!ascii_cache_matches(&bytes, &units));
                units[index] ^= 0xd800;
            }
            units.push(0);
            assert!(!ascii_cache_matches(&bytes, &units));
        }
    }
}
