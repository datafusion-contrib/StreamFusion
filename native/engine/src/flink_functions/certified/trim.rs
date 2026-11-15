use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
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
        // Flink's empty-set outcome depends on BinaryStringData's decoded-cache state.
        if set.is_empty() {
            return None;
        }
        // Without a decoded cache, Flink treats any set starting with space as just space.
        if set.starts_with(' ')
            && set.len() > 1
            && cache.as_ref().and_then(|cache| cache.value(row)).is_none()
        {
            return None;
        }
        let mask = set
            .bytes()
            .fold(0_u128, |mask, byte| mask | (1_u128 << byte));
        let value = source.value(row);
        let bytes = value.as_bytes();
        let mut start = 0;
        let mut end = bytes.len();
        if operation != 15 {
            while start < end && mask & (1_u128 << bytes[start]) != 0 {
                start += 1;
            }
        }
        if operation != 14 {
            while end > start && mask & (1_u128 << bytes[end - 1]) != 0 {
                end -= 1;
            }
        }
        output.append_value(&value[start..end]);
    }
    Some(Arc::new(output.finish()))
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
    fn leading_space_sets_require_the_original_decoded_cache() {
        let set: ArrayRef = Arc::new(StringArray::from(vec![" ab"]));
        let source: ArrayRef = Arc::new(StringArray::from(vec![" abXYZba "]));
        assert!(certified(13, set.clone(), source.clone(), &[]).is_none());
        let cache = string_cache_array([Some(&[32_u16, 97, 98][..])]);
        assert_eq!(
            certified(13, set, source, &[cache]).unwrap().as_ref(),
            &StringArray::from(vec!["XYZ"])
        );
    }
}
