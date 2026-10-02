use std::hash::Hash;
use std::sync::Arc;

use ahash::AHashSet;
use arrow::array::{
    Array, ArrayRef, BooleanArray, ListArray, PrimitiveArray, StringArray, UInt32Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(ScalarArrayDistinct {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ScalarArrayDistinct {
    signature: Signature,
}

impl ScalarUDFImpl for ScalarArrayDistinct {
    fn name(&self) -> &str {
        "flink_array_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [array @ DataType::List(field)]
                if matches!(
                    field.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Boolean
                        | DataType::Utf8
                ) =>
            {
                Ok(array.clone())
            }
            _ => exec_err!("ARRAY_DISTINCT requires a BOOLEAN, integer, or VARCHAR ARRAY"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(distinct, vec![])(&args.args)
    }
}

fn distinct(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("ARRAY_DISTINCT requires one argument");
    };
    let array = as_list_array(input)?;
    match array.value_type() {
        DataType::Int8 => distinct_primitive::<Int8Type>(array),
        DataType::Int16 => distinct_primitive::<Int16Type>(array),
        DataType::Int32 => distinct_primitive::<Int32Type>(array),
        DataType::Int64 => distinct_primitive::<Int64Type>(array),
        DataType::Boolean => {
            let values = array
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            distinct_values::<_, _, true>(
                array,
                values,
                |index| values.value(index),
                |value| value as usize,
            )
        }
        DataType::Utf8 => {
            let values = array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            distinct_values::<_, _, false>(
                array,
                values,
                |index| values.value(index),
                |value| value.len(),
            )
        }
        _ => exec_err!("ARRAY_DISTINCT requires a BOOLEAN, integer, or VARCHAR ARRAY"),
    }
}

fn distinct_primitive<T: ArrowPrimitiveType>(array: &ListArray) -> Result<ArrayRef>
where
    T::Native: Eq + Hash,
{
    let values = array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("integer array element type");
    distinct_values::<_, _, false>(
        array,
        values,
        |index| values.value(index),
        |value| value.as_usize(),
    )
}

fn distinct_values<K: Eq + Hash + Copy + Default, A: Array, const BOOLEAN: bool>(
    array: &ListArray,
    values: &A,
    value: impl Fn(usize) -> K,
    fingerprint: impl Fn(K) -> usize,
) -> Result<ArrayRef> {
    let input_offsets = array.value_offsets();
    let visible = (input_offsets[array.len()] - input_offsets[0]) as usize;
    let mut indices = None;
    let mut offsets = Vec::new();
    let mut seen = AHashSet::new();
    for row in 0..array.len() {
        let start = input_offsets[row] as usize;
        let end = input_offsets[row + 1] as usize;
        if array.is_null(row) {
            if start != end && indices.is_none() {
                begin_selection(
                    &mut indices,
                    &mut offsets,
                    input_offsets,
                    row,
                    start,
                    visible,
                );
            }
        } else {
            let mut seen_null = false;
            let mut small_values = [K::default(); 8];
            let mut small_len = 0;
            let mut small_fingerprint = 0u64;
            let small = end - start <= small_values.len();
            if !small && !BOOLEAN {
                seen.clear();
            }
            for index in start..end {
                let first = if values.is_null(index) {
                    !std::mem::replace(&mut seen_null, true)
                } else if BOOLEAN {
                    // Boolean fingerprints are exact: false is bit 0 and true is bit 1.
                    let bit = 1u64 << fingerprint(value(index));
                    let first = small_fingerprint & bit == 0;
                    small_fingerprint |= bit;
                    first
                } else if small {
                    let value = value(index);
                    let bit = 1u64 << (fingerprint(value) & 63);
                    if small_fingerprint & bit != 0 && small_values[..small_len].contains(&value) {
                        false
                    } else {
                        small_fingerprint |= bit;
                        small_values[small_len] = value;
                        small_len += 1;
                        true
                    }
                } else {
                    seen.insert(value(index))
                };
                if first {
                    if let Some(indices) = &mut indices {
                        indices.push(index as u32);
                    }
                } else if indices.is_none() {
                    begin_selection(
                        &mut indices,
                        &mut offsets,
                        input_offsets,
                        row,
                        index,
                        visible,
                    );
                }
            }
        }
        if let Some(indices) = &indices {
            offsets.push(indices.len() as i32);
        }
    }
    let Some(indices) = indices else {
        return Ok(Arc::new(array.clone()));
    };
    let output = arrow::compute::take(array.values().as_ref(), &UInt32Array::from(indices), None)?;
    let DataType::List(field) = array.data_type() else {
        unreachable!("list array type")
    };
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        output,
        array.nulls().cloned(),
    )?))
}

fn begin_selection(
    indices: &mut Option<Vec<u32>>,
    offsets: &mut Vec<i32>,
    source_offsets: &[i32],
    row: usize,
    prefix_end: usize,
    visible: usize,
) {
    if indices.is_none() {
        // Until the first removed value, retained indices are a contiguous visible prefix.
        let base = source_offsets[0];
        let mut selected = Vec::with_capacity(visible);
        selected.extend((base as usize..prefix_end).map(|index| index as u32));
        offsets.reserve_exact(source_offsets.len());
        offsets.extend(source_offsets[..=row].iter().map(|offset| offset - base));
        *indices = Some(selected);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanBuilder, ListBuilder, StringBuilder};

    fn strings(rows: Vec<Option<Vec<Option<&str>>>>) -> ListArray {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            if let Some(row) = row {
                for value in row {
                    builder.values().append_option(value);
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        builder.finish()
    }

    #[test]
    fn string_slices_keep_first_occurrences_nulls_and_raw_unicode_equality() {
        let mut large = vec![
            Some("ab"),
            Some("ba"),
            None,
            Some(""),
            Some("é"),
            Some("e\u{301}"),
            Some("\0"),
        ];
        large.extend(vec![Some("ab"); 57]);
        let array = strings(vec![
            Some(vec![Some("unused")]),
            Some(large),
            Some(vec![
                Some("x"),
                None,
                Some("x"),
                Some("\0"),
                Some("y"),
                Some("y"),
                None,
                Some(""),
            ]),
            Some(vec![]),
            None,
        ])
        .slice(1, 4);
        let expected = strings(vec![
            Some(vec![
                Some("ab"),
                Some("ba"),
                None,
                Some(""),
                Some("é"),
                Some("e\u{301}"),
                Some("\0"),
            ]),
            Some(vec![Some("x"), None, Some("\0"), Some("y"), Some("")]),
            Some(vec![]),
            None,
        ]);
        let output = distinct(&[Arc::new(array)]).unwrap();
        assert_eq!(output.as_ref(), &expected);
    }

    #[test]
    fn boolean_slices_reset_membership_and_keep_null_containers() {
        let mut input = ListBuilder::new(BooleanBuilder::new());
        let rows = [
            Some(vec![Some(false); 3]),
            Some(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true),
                None,
                Some(false),
            ]),
            Some(vec![Some(false), Some(false), None, Some(true), None]),
            Some(vec![]),
            None,
        ];
        for row in rows {
            if let Some(row) = row {
                for value in row {
                    input.values().append_option(value);
                }
                input.append(true);
            } else {
                input.append(false);
            }
        }
        let array = input.finish().slice(1, 4);
        let output = distinct(&[Arc::new(array.clone())]).unwrap();
        let result = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(result.data_type(), array.data_type());
        assert_eq!(result.value_offsets(), &[0, 3, 6, 6, 6]);
        assert!(result.is_null(3));
        assert_eq!(
            result.values().as_ref(),
            &BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(false),
                None,
                Some(true)
            ])
        );
    }

    #[test]
    fn lazy_selection_excludes_hidden_null_values_after_an_unchanged_prefix() {
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::Field;
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "unused", "a", "b", "hidden1", "hidden2", "c", "c",
        ]));
        let array = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 1, 3, 5, 7].into()),
            values,
            Some(NullBuffer::from(vec![true, true, false, true])),
        )
        .slice(1, 3);
        let output = distinct(&[Arc::new(array)]).unwrap();
        let expected = strings(vec![
            Some(vec![Some("a"), Some("b")]),
            None,
            Some(vec![Some("c")]),
        ]);
        assert_eq!(output.as_ref(), &expected);
    }

    #[test]
    fn unchanged_strings_share_buffers_and_preserve_nonnull_field_metadata() {
        use arrow::datatypes::Field;
        let field = Arc::new(
            Field::new("item", DataType::Utf8, false)
                .with_metadata([("source".into(), "test".into())].into_iter().collect()),
        );
        let mut builder = ListBuilder::new(StringBuilder::new()).with_field(field.clone());
        for value in ["unused", "a", "b"] {
            builder.values().append_value(value);
            builder.append(true);
        }
        let array = builder.finish().slice(1, 2);
        let output = distinct(&[Arc::new(array.clone())]).unwrap();
        let result = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(result.data_type(), &DataType::List(field));
        assert_eq!(
            result.values().to_data().buffers()[0].as_ptr(),
            array.values().to_data().buffers()[0].as_ptr()
        );
        assert_eq!(result, &array);
    }
}
