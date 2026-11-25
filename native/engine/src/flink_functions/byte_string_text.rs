use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int32Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::{as_binary_array, as_string_array},
    exec_err, Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(crate) fn length_function() -> ScalarUDF {
    super::udf(
        "flink_char_length_bytes",
        vec![DataType::Binary],
        DataType::Int32,
        character_length,
    )
}

pub(crate) fn position_function(mut inputs: Vec<DataType>) -> ScalarUDF {
    for data_type in &mut inputs {
        if *data_type != DataType::Binary {
            *data_type = DataType::Utf8;
        }
    }
    super::udf("flink_position_bytes", inputs, DataType::Int32, position)
}

pub(crate) fn space_trim_function(operation: i64) -> ScalarUDF {
    use crate::expression_protocol::scalar_op;
    let (name, trim): (_, fn(&[ArrayRef]) -> Result<ArrayRef>) = match operation {
        scalar_op::LTRIM_WHITESPACE => ("flink_ltrim_space_bytes", space_trim::<true, false>),
        scalar_op::RTRIM_WHITESPACE => ("flink_rtrim_space_bytes", space_trim::<false, true>),
        _ => ("flink_btrim_space_bytes", space_trim::<true, true>),
    };
    super::udf(name, vec![DataType::Binary], DataType::Binary, trim)
}

fn space_trim<const LEADING: bool, const TRAILING: bool>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let input = as_binary_array(&args[0])?;
    let offsets = input.value_offsets();
    let mut output =
        BinaryBuilder::with_capacity(input.len(), (offsets[input.len()] - offsets[0]) as usize);
    for value in input.iter() {
        let Some(value) = value else {
            output.append_null();
            continue;
        };
        let start = if LEADING {
            value
                .iter()
                .position(|&byte| byte != b' ')
                .unwrap_or(value.len())
        } else {
            0
        };
        let remaining = &value[start..];
        let end = if TRAILING {
            remaining
                .iter()
                .rposition(|&byte| byte != b' ')
                .map_or(0, |index| index + 1)
        } else {
            remaining.len()
        };
        output.append_value(&remaining[..end]);
    }
    Ok(Arc::new(output.finish()))
}

/// Build an Arrow text view only after checking both the backing buffer and the row boundaries.
/// A byte sequence split across two rows is malformed even when the concatenated buffer is valid.
pub(super) fn utf8_view(input: &BinaryArray) -> Option<StringArray> {
    let offsets = input.value_offsets();
    let end = offsets[offsets.len() - 1] as usize;
    let values = input.values().slice_with_length(0, end);
    let text = simdutf8::basic::from_utf8(&values).ok()?;
    if !offsets
        .iter()
        .all(|&offset| text.is_char_boundary(offset as usize))
    {
        return None;
    }
    // SAFETY: the BinaryArray guarantees the offsets and null buffer shape. The entire
    // shared value buffer is valid UTF-8 and each offset is on a character boundary.
    Some(unsafe {
        StringArray::new_unchecked(input.offsets().clone(), values, input.nulls().cloned())
    })
}

/// Mark the rows that cannot safely be exposed as Utf8. Include non-empty null spans because
/// the retained Arrow text view also requires its unused bytes to satisfy the UTF-8 invariant.
pub(crate) fn mark_invalid_utf8(input: &BinaryArray, invalid: &mut [bool]) {
    if utf8_view(input).is_none() {
        for (row, invalid) in invalid.iter_mut().enumerate() {
            *invalid |= simdutf8::basic::from_utf8(input.value(row)).is_err();
        }
    }
}

pub(super) enum TextColumn<'a> {
    Utf8(&'a StringArray),
    Validated(StringArray),
    Mixed(&'a BinaryArray),
}

pub(super) enum TextValue<'a> {
    Utf8(&'a str),
    Malformed(&'a [u8]),
}

impl<'a> TextColumn<'a> {
    fn binary(input: &'a BinaryArray) -> Self {
        utf8_view(input).map_or(Self::Mixed(input), Self::Validated)
    }

    pub(super) fn new(input: &'a ArrayRef) -> Result<Self> {
        if input.data_type() == &DataType::Binary {
            Ok(Self::binary(as_binary_array(input)?))
        } else {
            Ok(Self::Utf8(as_string_array(input)?))
        }
    }

    pub(super) fn get(&self, row: usize) -> Option<TextValue<'_>> {
        match self {
            Self::Utf8(text) => text_at(text, row),
            Self::Validated(text) => text_at(text, row),
            Self::Mixed(binary) => (!binary.is_null(row)).then(|| {
                let bytes = binary.value(row);
                match simdutf8::basic::from_utf8(bytes) {
                    Ok(text) => TextValue::Utf8(text),
                    Err(_) => TextValue::Malformed(bytes),
                }
            }),
        }
    }
}

fn text_at(input: &StringArray, row: usize) -> Option<TextValue<'_>> {
    (!input.is_null(row)).then(|| TextValue::Utf8(input.value(row)))
}

fn character_length(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("CHAR_LENGTH expects one character argument");
    };
    let input = as_binary_array(input)?;
    let values = if input.is_ascii() {
        input
            .value_offsets()
            .windows(2)
            .map(|offsets| offsets[1] - offsets[0])
            .collect::<Vec<_>>()
    } else {
        let text = TextColumn::binary(input);
        (0..input.len())
            .map(|row| match text.get(row) {
                Some(TextValue::Utf8(value)) => value.chars().count() as i32,
                Some(TextValue::Malformed(bytes)) => flink_char_starts(bytes).count() as i32,
                None => 0,
            })
            .collect()
    };
    Ok(Arc::new(Int32Array::new(
        values.into(),
        input.nulls().cloned(),
    )))
}

fn position(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [needle, haystack] = args else {
        return exec_err!("POSITION expects a substring and a string");
    };
    let needles = TextColumn::new(needle)?;
    let haystacks = TextColumn::new(haystack)?;
    Ok(Arc::new(Int32Array::from_iter((0..haystack.len()).map(
        |row| {
            let needle = needles.get(row)?;
            let haystack = haystacks.get(row)?;
            Some(match (needle, haystack) {
                (TextValue::Utf8(needle), TextValue::Utf8(haystack)) => {
                    memchr::memmem::find(haystack.as_bytes(), needle.as_bytes())
                        .map_or(0, |index| haystack[..index].chars().count() as i32 + 1)
                }
                (needle, haystack) => {
                    let needle = needle.as_bytes();
                    let haystack = haystack.as_bytes();
                    if needle.is_empty() {
                        return Some(1);
                    }
                    if needle.len() > haystack.len() {
                        return Some(0);
                    }
                    flink_char_starts(haystack)
                        .take_while(|&index| index <= haystack.len() - needle.len())
                        .position(|index| haystack[index..].starts_with(needle))
                        .map_or(0, |index| index as i32 + 1)
                }
            })
        },
    ))))
}

impl<'a> TextValue<'a> {
    pub(super) fn as_bytes(&self) -> &'a [u8] {
        match self {
            Self::Utf8(text) => text.as_bytes(),
            Self::Malformed(bytes) => bytes,
        }
    }
}

// Released Flink's BinaryStringData advances using the first byte alone. A lead byte may
// skip arbitrary following bytes or advance beyond the end of a truncated final sequence.
fn flink_char_starts(bytes: &[u8]) -> impl Iterator<Item = usize> + '_ {
    let mut offset = 0;
    std::iter::from_fn(move || {
        let start = offset;
        offset += flink_char_width(*bytes.get(offset)?);
        Some(start)
    })
}

pub(super) fn flink_char_width(first: u8) -> usize {
    match first {
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf7 => 4,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};

    #[test]
    fn utf8_view_reuses_values_offsets_and_nulls() {
        let input = BinaryArray::from_iter([
            Some(b"skip".as_slice()),
            Some("中🙂".as_bytes()),
            None,
            Some(b""),
            Some(b"\xff"),
        ])
        .slice(1, 3);
        let view = utf8_view(&input).unwrap();
        assert_eq!(view.values().as_ptr(), input.values().as_ptr());
        assert_eq!(
            view.value_offsets().as_ptr(),
            input.value_offsets().as_ptr()
        );
        assert_eq!(view.nulls(), input.nulls());
        assert_eq!(
            view.iter().collect::<Vec<_>>(),
            [Some("中🙂"), None, Some("")]
        );
        view.to_data().validate_full().unwrap();
    }

    #[test]
    fn whole_buffer_validity_cannot_hide_split_rows() {
        let input = BinaryArray::from_vec(vec![b"\xc3".as_slice(), b"\xa9"]);
        assert!(std::str::from_utf8(input.value_data()).is_ok());
        assert!(utf8_view(&input).is_none());
    }

    #[test]
    fn batch_validation_combines_invalid_rows_from_required_columns() {
        let first = BinaryArray::from_vec(vec!["é".as_bytes(), b"\xc3", b"\xa9", b""]);
        let next = BinaryArray::from_iter([Some(b"\xff".as_slice()), None, Some(b"a"), Some(b"b")]);
        let mut invalid = [false; 4];
        mark_invalid_utf8(&first, &mut invalid);
        assert_eq!(invalid, [false, true, true, false]);
        mark_invalid_utf8(&next, &mut invalid);
        assert_eq!(invalid, [true, true, true, false]);
    }

    #[test]
    fn malformed_null_slot_does_not_change_character_counts() {
        let input = BinaryArray::new(
            OffsetBuffer::new(vec![0, 1, 3, 5].into()),
            Buffer::from(&b"\xff\xc3\xa9\xe0Q"[..]),
            Some(NullBuffer::from(vec![false, true, true])),
        );
        let result = character_length(&[Arc::new(input)]).unwrap();
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![None, Some(1), Some(1)]),
        );
    }

    #[test]
    fn mixed_position_matches_bytes_only_at_flink_character_starts() {
        let needles = BinaryArray::from_iter([
            Some(b"Q".as_slice()),
            Some(b"\xc3"),
            Some(b"\xa9"),
            Some(b"a"),
            Some(b""),
            None,
        ]);
        let haystacks = BinaryArray::from_iter([
            Some(b"\xe0QQ".as_slice()),
            Some("é".as_bytes()),
            Some("é".as_bytes()),
            Some("🙂a".as_bytes()),
            Some(b"\xff"),
            Some(b"abc"),
        ]);
        let result = position(&[Arc::new(needles), Arc::new(haystacks)]).unwrap();
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(0), Some(1), Some(0), Some(2), Some(1), None]),
        );
    }
}
