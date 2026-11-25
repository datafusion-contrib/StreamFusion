use crate::expression_protocol::certified_op;
use crate::flink_functions::byte_string_text::flink_char_width;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, StringArray, StringBuilder,
    StructArray,
};
use arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::ops::Range;
use std::sync::Arc;
use streamfusion_bridge::string_cache::JavaStringCacheColumn;
use streamfusion_bridge::FlinkException;

const SET_CASED: i64 = 1 << 8;
const SET_SHARED_EMPTY_CACHED: i64 = 1 << 10;

pub(crate) fn function(operation: i64, literal_set: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(Trim {
        operation: operation & 0xff,
        set_cased: operation & SET_CASED != 0,
        shared_empty_cached: operation & SET_SHARED_EMPTY_CACHED != 0,
        literal_set,
        signature: Signature::any(4, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Trim {
    operation: i64,
    set_cased: bool,
    shared_empty_cached: bool,
    literal_set: bool,
    signature: Signature,
}

impl ScalarUDFImpl for Trim {
    fn name(&self) -> &str {
        "flink_trim_characters"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(
            crate::flink_functions::collection_string::represented_byte_type(&args[1])
                .unwrap_or(&args[1])
                .clone(),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = args
            .args
            .iter()
            .map(|value| value.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let invalid = || exec_datafusion_err!("Invalid operands for Flink trim");
        let represented = |array: &ArrayRef| {
            crate::flink_functions::collection_string::represented_byte_type(array.data_type())
                .is_some()
        };
        let set_carrier = represented(&arrays[0])
            .then(|| arrays[0].as_any().downcast_ref::<StructArray>().unwrap());
        let value_carrier = represented(&arrays[1])
            .then(|| arrays[1].as_any().downcast_ref::<StructArray>().unwrap());
        let sets = Bytes::new(set_carrier.map_or(&arrays[0], |value| value.column(0)))
            .ok_or_else(invalid)?;
        let values = Bytes::new(value_carrier.map_or(&arrays[1], |value| value.column(0)))
            .ok_or_else(invalid)?;
        let cache_array = set_carrier.map_or(&arrays[2], |value| value.column(1));
        let caches = (cache_array.data_type() != &DataType::Null)
            .then(|| JavaStringCacheColumn::new(cache_array));
        let decoded = arrays[3]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(invalid)?;
        let batch_ascii = sets.is_ascii() && values.is_ascii();
        let bounds = |row| -> Result<Option<Range<usize>>> {
            if set_carrier.is_some_and(|array| array.is_null(row))
                || value_carrier.is_some_and(|array| array.is_null(row))
            {
                return Ok(None);
            }
            let (Some(set), Some(value)) = (sets.get(row), values.get(row)) else {
                return Ok(None);
            };
            let cached = caches.as_ref().and_then(|caches| caches.value(row));
            let spaces = if self.literal_set || decoded.value(row) && cached.is_none() {
                set == b" "
            } else if let Some(cached) = cached {
                cached == [u16::from(b' ')]
            } else if self.set_cased && set.iter().any(|&byte| flink_char_width(byte) > 1) {
                // The preceding case conversion decoded the set. Its multibyte or
                // malformed sequence makes it different from the single Java space.
                false
            } else if let Some(first) = set.first() {
                *first == b' '
            } else if self.shared_empty_cached {
                false
            } else {
                // Generated Flink Calc copies STRING input fields into their own segments.
                // A cold empty argument has a zero-sized segment when byteAt(0) runs.
                return Err(trim_error(FlinkException::arithmetic("/ by zero".into())));
            };
            let range = if spaces {
                trim_bounds(self.operation, value, 1_u128 << b' ')
            } else if batch_ascii || (set.is_ascii() && value.is_ascii()) {
                let mask = set
                    .iter()
                    .fold(0_u128, |mask, byte| mask | (1_u128 << byte));
                trim_bounds(self.operation, value, mask)
            } else if let Ok(text) = simdutf8::basic::from_utf8(value) {
                trim_unicode(self.operation, text, set)
            } else {
                trim_malformed(self.operation, value, set)?
            };
            Ok(Some(range))
        };
        let output = if let Bytes::Utf8(source) = values {
            let mut output = StringBuilder::with_capacity(values.len(), values.value_capacity());
            for row in 0..values.len() {
                match bounds(row)? {
                    Some(range) => output.append_value(&source.value(row)[range]),
                    None => output.append_null(),
                }
            }
            Arc::new(output.finish()) as ArrayRef
        } else {
            let mut output = BinaryBuilder::with_capacity(values.len(), values.value_capacity());
            for row in 0..values.len() {
                match bounds(row)? {
                    Some(range) => {
                        output.append_value(&values.get(row).expect("nonnull trim source")[range])
                    }
                    None => output.append_null(),
                }
            }
            Arc::new(output.finish()) as ArrayRef
        };
        Ok(ColumnarValue::Array(output))
    }
}

fn trim_error(error: FlinkException) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn trim_malformed(operation: i64, value: &[u8], set: &[u8]) -> Result<Range<usize>> {
    let member = |text: &[u8], offset: usize, width: usize| -> Result<bool> {
        let part = text
            .get(offset..offset + width)
            .ok_or_else(|| trim_error(FlinkException::index_out_of_bounds()))?;
        Ok(memchr::memmem::find(set, part).is_some())
    };
    let mut start = 0;
    if operation != certified_op::RTRIM {
        while let Some(&byte) = value.get(start) {
            let width = flink_char_width(byte);
            if !member(value, start, width)? {
                break;
            }
            start += width;
        }
        if operation == certified_op::LTRIM {
            return Ok(start..value.len());
        }
    }
    let right = &value[start..];
    let mut spans = Vec::new();
    let mut offset = 0;
    while let Some(&byte) = right.get(offset) {
        let width = flink_char_width(byte);
        spans.push((offset, width));
        offset += width;
    }
    let mut remaining = right.len();
    for (offset, width) in spans.into_iter().rev() {
        if !member(right, offset, width)? {
            break;
        }
        remaining = remaining.saturating_sub(width);
    }
    Ok(start..start + remaining)
}

pub(super) fn evaluate(
    operation: i64,
    arrays: &[ArrayRef],
    caches: &[ArrayRef],
    target: &DataType,
) -> Option<ArrayRef> {
    let [set, source] = arrays else { return None };
    let sets = Bytes::new(set)?;
    let values = Bytes::new(source)?;
    if sets.len() != values.len() {
        return None;
    }
    let cache = caches
        .first()
        .filter(|cache| cache.data_type() != &DataType::Null)
        .map(JavaStringCacheColumn::new);
    let ascii_batch = sets.is_ascii() && values.is_ascii();
    let bounds = |row| {
        let (Some(set), Some(value)) = (sets.get(row), values.get(row)) else {
            return Some(None);
        };
        let cached = cache.as_ref().and_then(|cache| cache.value(row));
        let space_only = match cached {
            Some(units) => units == [u16::from(b' ')],
            // Flink probes byte zero, so an empty cold set can fail in the released evaluator.
            None => *set.first()? == b' ',
        };
        if space_only {
            return Some(Some(trim_bounds(operation, value, 1_u128 << b' ')));
        }
        if ascii_batch || (set.is_ascii() && value.is_ascii()) {
            let mask = set
                .iter()
                .fold(0_u128, |mask, byte| mask | (1_u128 << byte));
            return Some(Some(trim_bounds(operation, value, mask)));
        }
        Some(Some(match simdutf8::basic::from_utf8(value) {
            Ok(text) => trim_unicode(operation, text, set),
            Err(_) => trim_binary(operation, value, set)?,
        }))
    };

    match target {
        DataType::Binary => {
            let mut output = BinaryBuilder::with_capacity(values.len(), values.value_capacity());
            for row in 0..values.len() {
                match bounds(row)? {
                    Some(range) => output.append_value(&values.get(row)?[range]),
                    None => output.append_null(),
                }
            }
            Some(Arc::new(output.finish()))
        }
        DataType::Utf8 => {
            let source = source.as_any().downcast_ref::<StringArray>()?;
            let mut output = StringBuilder::with_capacity(values.len(), values.value_capacity());
            for row in 0..values.len() {
                match bounds(row)? {
                    Some(range) => output.append_value(&source.value(row)[range]),
                    None => output.append_null(),
                }
            }
            Some(Arc::new(output.finish()))
        }
        _ => None,
    }
}

fn trim_bounds(operation: i64, bytes: &[u8], mask: u128) -> Range<usize> {
    let contains = |byte: u8| byte < 128 && mask & (1_u128 << byte) != 0;
    let mut start = 0;
    let mut end = bytes.len();
    if operation != certified_op::RTRIM {
        while start < end && contains(bytes[start]) {
            start += 1;
        }
    }
    if operation != certified_op::LTRIM {
        while end > start && contains(bytes[end - 1]) {
            end -= 1;
        }
    }
    start..end
}

fn trim_unicode(operation: i64, text: &str, set: &[u8]) -> Range<usize> {
    // Flink copies each source character as binary and searches for its bytes in the trim set.
    // Only the space-only check above consults the original Java cache.
    let contains = |offset: usize, ch: char| {
        memchr::memmem::find(set, &text.as_bytes()[offset..offset + ch.len_utf8()]).is_some()
    };
    let start = if operation == certified_op::RTRIM {
        0
    } else {
        text.char_indices()
            .find(|&(offset, ch)| !contains(offset, ch))
            .map_or(text.len(), |(offset, _)| offset)
    };
    let end = if operation == certified_op::LTRIM {
        text.len()
    } else {
        text[start..]
            .char_indices()
            .rev()
            .find(|&(offset, ch)| !contains(start + offset, ch))
            .map_or(start, |(offset, ch)| start + offset + ch.len_utf8())
    };
    start..end
}

fn trim_binary(operation: i64, value: &[u8], set: &[u8]) -> Option<Range<usize>> {
    let mut offset = 0;
    let mut start = 0;
    let mut end = 0;
    while let Some(&first) = value.get(offset) {
        let next = offset + flink_char_width(first);
        // Flink can read past the logical string here. Arrow does not retain those bytes.
        let part = value.get(offset..next)?;
        let member = memchr::memmem::find(set, part).is_some();
        if operation != certified_op::RTRIM && start == offset && member {
            start = next;
        }
        if operation == certified_op::LTRIM && !member {
            return Some(start..value.len());
        }
        if !member {
            end = next;
        }
        offset = next;
    }
    Some(start..end.max(start))
}

enum Bytes<'a> {
    Binary(&'a BinaryArray),
    Utf8(&'a StringArray),
}

impl<'a> Bytes<'a> {
    fn new(array: &'a ArrayRef) -> Option<Self> {
        let array = array.as_any();
        if let Some(binary) = array.downcast_ref::<BinaryArray>() {
            Some(Self::Binary(binary))
        } else {
            array.downcast_ref::<StringArray>().map(Self::Utf8)
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Binary(array) => array.len(),
            Self::Utf8(array) => array.len(),
        }
    }

    fn is_ascii(&self) -> bool {
        match self {
            Self::Binary(array) => array.is_ascii(),
            Self::Utf8(array) => array.is_ascii(),
        }
    }

    fn get(&self, row: usize) -> Option<&'a [u8]> {
        match self {
            Self::Binary(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Utf8(array) => (!array.is_null(row)).then(|| array.value(row).as_bytes()),
        }
    }

    fn value_capacity(&self) -> usize {
        let offsets = match self {
            Self::Binary(array) => array.value_offsets(),
            Self::Utf8(array) => array.value_offsets(),
        };
        (offsets[self.len()] - offsets[0]) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use streamfusion_bridge::string_cache::string_cache_array;

    fn certified(
        operation: i64,
        set: ArrayRef,
        source: ArrayRef,
        caches: &[ArrayRef],
    ) -> Option<ArrayRef> {
        super::super::evaluate(operation, &[set, source], caches, &DataType::Utf8)
    }

    #[test]
    fn directions_sets_nulls_slices_and_non_regex_punctuation() {
        let set: ArrayRef = Arc::new(StringArray::from(vec![
            Some("unused"),
            Some("ab"),
            Some("[]+"),
            Some("a"),
            None,
            Some("a"),
            Some("a"),
        ]));
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            Some("unused"),
            Some("abXYZba"),
            Some("[+XYZ+]"),
            Some("aaa"),
            Some("a"),
            None,
            Some(""),
        ]));
        for (operation, first, second) in [
            (13, "XYZ", "XYZ"),
            (14, "XYZba", "XYZ+]"),
            (15, "abXYZ", "[+XYZ"),
        ] {
            let output = certified(operation, set.slice(1, 6), source.slice(1, 6), &[]).unwrap();
            assert_eq!(
                output.as_ref(),
                &StringArray::from(vec![
                    Some(first),
                    Some(second),
                    Some(""),
                    None,
                    None,
                    Some("")
                ])
            );
        }
        assert_eq!(
            certified(13, set.slice(0, 0), source.slice(0, 0), &[])
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn cold_empty_set_retains_the_host_evaluator() {
        assert!(certified(
            certified_op::TRIM,
            Arc::new(StringArray::from(vec![""])),
            Arc::new(StringArray::from(vec!["abc"])),
            &[]
        )
        .is_none());
    }

    #[test]
    fn byte_membership_does_not_require_original_java_cache_equivalence() {
        let set: ArrayRef = Arc::new(StringArray::from(vec!["?"]));
        let source: ArrayRef = Arc::new(StringArray::from(vec!["?abc?"]));
        let unequal = string_cache_array([Some(&[0xd800_u16][..])]);
        assert_eq!(
            certified(certified_op::TRIM, set, source, &[unequal])
                .unwrap()
                .as_ref(),
            &StringArray::from(vec!["abc"])
        );
    }

    #[test]
    fn mixed_cache_and_unicode_rows_match_for_text_and_binary_outputs() {
        let sets = [" 🙂中", " 🙂中", "🙂中", " ", "", "z"];
        let values = [
            " 🙂中x中🙂 ",
            " 🙂中x中🙂 ",
            " 🙂x中",
            "\t x \t",
            "中",
            "中",
        ];
        let cached_set: Vec<_> = sets[1].encode_utf16().collect();
        let caches = string_cache_array([
            None,
            Some(cached_set.as_slice()),
            None,
            None,
            Some(&[]),
            None,
        ]);
        for (operation, expected) in [
            (
                certified_op::TRIM,
                ["🙂中x中🙂", "x", " 🙂x", "\t x \t", "中", "中"],
            ),
            (
                certified_op::LTRIM,
                ["🙂中x中🙂 ", "x中🙂 ", " 🙂x中", "\t x \t", "中", "中"],
            ),
            (
                certified_op::RTRIM,
                [" 🙂中x中🙂", " 🙂中x", " 🙂x", "\t x \t", "中", "中"],
            ),
        ] {
            let text = certified(
                operation,
                Arc::new(StringArray::from(sets.to_vec())),
                Arc::new(StringArray::from(values.to_vec())),
                &[caches.clone()],
            )
            .unwrap();
            assert_eq!(text.as_ref(), &StringArray::from(expected.to_vec()));
            let binary = evaluate(
                operation,
                &[
                    Arc::new(BinaryArray::from_iter_values(sets.map(str::as_bytes))),
                    Arc::new(BinaryArray::from_iter_values(values.map(str::as_bytes))),
                ],
                &[caches.clone()],
                &DataType::Binary,
            )
            .unwrap();
            assert_eq!(
                binary.as_ref(),
                &BinaryArray::from_iter_values(expected.map(str::as_bytes))
            );
        }
    }

    #[test]
    fn malformed_binary_membership_uses_flink_lead_byte_widths() {
        let set = b"\xe0QQ\xff";
        let value = b"\xe0QQx\xff";
        for (operation, expected) in [
            (certified_op::TRIM, b"x".as_slice()),
            (certified_op::LTRIM, b"x\xff".as_slice()),
            (certified_op::RTRIM, b"\xe0QQx".as_slice()),
        ] {
            assert_eq!(
                &value[trim_binary(operation, value, set).unwrap()],
                expected
            );
        }
        let truncated = b"x\xe0";
        assert_eq!(trim_binary(certified_op::LTRIM, truncated, set), Some(0..2));
        assert_eq!(trim_binary(certified_op::RTRIM, truncated, set), None);
    }

    #[test]
    fn cached_empty_set_leaves_the_source_unchanged() {
        let set: ArrayRef = Arc::new(StringArray::from(vec![""]));
        let source: ArrayRef = Arc::new(StringArray::from(vec!["abc"]));
        let cache = string_cache_array([Some(&[][..])]);
        assert_eq!(
            certified(13, set, source, &[cache]).unwrap().as_ref(),
            &StringArray::from(vec!["abc"])
        );
    }

    #[test]
    fn leading_space_sets_follow_the_original_decoded_cache() {
        let set: ArrayRef = Arc::new(StringArray::from(vec![" ab"]));
        let source: ArrayRef = Arc::new(StringArray::from(vec![" abXYZba "]));
        assert_eq!(
            certified(13, set.clone(), source.clone(), &[])
                .unwrap()
                .as_ref(),
            &StringArray::from(vec!["abXYZba"])
        );
        let cache = string_cache_array([Some(&[32_u16, 97, 98][..])]);
        assert_eq!(
            certified(13, set, source, &[cache]).unwrap().as_ref(),
            &StringArray::from(vec!["XYZ"])
        );
    }
}
