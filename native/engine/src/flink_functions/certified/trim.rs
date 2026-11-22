use crate::expression_protocol::certified_op;
use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, StringArray, StringBuilder};
use std::sync::Arc;
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

// The caller has proved ASCII bytes and equivalent original Java caches for the whole batch.
pub(super) fn evaluate(
    operation: i64,
    arrays: &[ArrayRef],
    caches: &[ArrayRef],
) -> Option<ArrayRef> {
    let [set, source] = arrays else { return None };
    let set = set.as_any().downcast_ref::<StringArray>()?;
    let source = source.as_any().downcast_ref::<StringArray>()?;
    if set.len() != source.len() {
        return None;
    }
    let mut output = StringBuilder::with_capacity(source.len(), source.value_data().len());
    let cache = caches
        .first()
        .filter(|cache| cache.data_type() != &arrow::datatypes::DataType::Null)
        .map(JavaStringCacheColumn::new);
    for row in 0..source.len() {
        if set.is_null(row) || source.is_null(row) {
            output.append_null();
            continue;
        }
        let set = set.value(row);
        let cached = cache.as_ref().and_then(|cache| cache.value(row)).is_some();
        // Flink's uncached empty set can fail when it checks the first byte.
        if set.is_empty() && !cached {
            return None;
        }
        // An uncached set starting with a space uses Flink's space-only trim.
        let mask = if set.starts_with(' ') && !cached {
            1_u128 << b' '
        } else {
            set.bytes()
                .fold(0_u128, |mask, byte| mask | (1_u128 << byte))
        };
        let value = source.value(row);
        let (start, end) = trim_bounds(operation, value.as_bytes(), mask);
        output.append_value(&value[start..end]);
    }
    Some(Arc::new(output.finish()))
}

/// Space-only trim compares the literal space byte, even on malformed inputs. An ASCII
/// set and source can also use byte membership. Everything else retains Flink traversal.
pub(super) fn evaluate_bytes(
    operation: i64,
    arrays: &[ArrayRef],
    caches: &[ArrayRef],
) -> Option<ArrayRef> {
    let [set, source] = arrays else { return None };
    let set = Bytes::new(set)?;
    let source = Bytes::new(source)?;
    if set.len() != source.len() {
        return None;
    }
    let cache = caches
        .first()
        .filter(|cache| cache.data_type() != &arrow::datatypes::DataType::Null)
        .map(JavaStringCacheColumn::new);
    let mut output = BinaryBuilder::with_capacity(source.len(), source.value_capacity());
    for row in 0..source.len() {
        let (Some(set), Some(source)) = (set.get(row), source.get(row)) else {
            output.append_null();
            continue;
        };
        let space_only = match cache.as_ref().and_then(|cache| cache.value(row)) {
            Some(units) => units == [u16::from(b' ')],
            None => *set.first()? == b' ',
        };
        let mask = if space_only {
            1_u128 << b' '
        } else if set.is_ascii() && source.is_ascii() {
            set.iter()
                .fold(0_u128, |mask, byte| mask | (1_u128 << byte))
        } else {
            return None;
        };
        let (start, end) = trim_bounds(operation, source, mask);
        output.append_value(&source[start..end]);
    }
    Some(Arc::new(output.finish()))
}

fn trim_bounds(operation: i64, bytes: &[u8], mask: u128) -> (usize, usize) {
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
    (start, end)
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
    fn uncertified_batches_retain_the_complete_host_evaluator() {
        for (set, source) in [("", "abc"), ("a", "\u{4e2d}"), ("\u{4e2d}", "a")] {
            assert!(certified(
                13,
                Arc::new(StringArray::from(vec![set])),
                Arc::new(StringArray::from(vec![source])),
                &[]
            )
            .is_none());
        }
        let set: ArrayRef = Arc::new(StringArray::from(vec!["?"]));
        let source: ArrayRef = Arc::new(StringArray::from(vec!["?abc?"]));
        let unequal = string_cache_array([Some(&[0xd800_u16][..])]);
        assert!(certified(13, set.clone(), source.clone(), &[unequal]).is_none());
        let equal = string_cache_array([Some(&[63_u16][..])]);
        assert_eq!(
            certified(13, set, source, &[equal]).unwrap().as_ref(),
            &StringArray::from(vec!["abc"])
        );
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
