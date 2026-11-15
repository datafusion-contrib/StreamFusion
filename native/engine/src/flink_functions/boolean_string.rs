use std::{borrow::Cow, sync::Arc};

use arrow::array::{new_null_array, Array, ArrayRef, BooleanArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(BooleanString {
        signature: Signature::any(3, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BooleanString {
    signature: Signature,
}

impl ScalarUDFImpl for BooleanString {
    fn name(&self) -> &str {
        "flink_boolean_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Boolean, DataType::Boolean, DataType::Int32] => Ok(DataType::Utf8),
            _ => exec_err!("Boolean formatting expects BOOLEAN and literal cast options"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input, ColumnarValue::Scalar(ScalarValue::Boolean(Some(legacy))), ColumnarValue::Scalar(ScalarValue::Int32(Some(length)))] =
            args.args.as_slice()
        else {
            return exec_err!("Boolean formatting expects BOOLEAN and literal cast options");
        };
        self.return_type(&[input.data_type(), DataType::Boolean, DataType::Int32])?;
        if *length == 0 || *length == i32::MIN {
            return exec_err!("Boolean formatting requires a positive character length");
        }
        match input {
            ColumnarValue::Scalar(ScalarValue::Boolean(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(value.map(|value| word(value, *legacy, *length).into_owned())),
            )),
            ColumnarValue::Array(input) => {
                let input = input.as_any().downcast_ref::<BooleanArray>().unwrap();
                if input.is_empty() || input.null_count() == input.len() {
                    return Ok(ColumnarValue::Array(new_null_array(
                        &DataType::Utf8,
                        input.len(),
                    )));
                }
                let yes = word(true, *legacy, *length);
                let no = word(false, *legacy, *length);
                if yes.len() == 4 && no.len() == 5 {
                    return boolean_words(input, *legacy).map(ColumnarValue::Array);
                }
                let mut offsets = Vec::with_capacity(input.len() + 1);
                let mut bytes =
                    Vec::with_capacity(input.len().saturating_mul(yes.len().max(no.len())));
                offsets.push(0_i32);
                for value in input {
                    if let Some(value) = value {
                        bytes.extend_from_slice(if value { yes.as_bytes() } else { no.as_bytes() });
                    }
                    let Ok(offset) = i32::try_from(bytes.len()) else {
                        return exec_err!(
                            "Boolean string batch exceeds Arrow UTF-8 offset capacity"
                        );
                    };
                    offsets.push(offset);
                }
                let output: ArrayRef = Arc::new(StringArray::new(
                    OffsetBuffer::new(offsets.into()),
                    bytes.into(),
                    input.nulls().cloned(),
                ));
                Ok(ColumnarValue::Array(output))
            }
            _ => exec_err!("Boolean formatting requires a BOOLEAN input"),
        }
    }
}

#[derive(Clone, Copy)]
struct WordBlock {
    bytes: [u8; 40],
    ends: [u8; 8],
}

const fn word_blocks(legacy: bool) -> [WordBlock; 256] {
    let mut blocks = [WordBlock {
        bytes: [0; 40],
        ends: [0; 8],
    }; 256];
    let mut bits = 0;
    while bits < blocks.len() {
        let mut end = 0;
        let mut row = 0;
        while row < 8 {
            let text: &[u8] = match (bits & (1 << row) != 0, legacy) {
                (true, true) => b"true",
                (false, true) => b"false",
                (true, false) => b"TRUE",
                (false, false) => b"FALSE",
            };
            let mut byte = 0;
            while byte < text.len() {
                blocks[bits].bytes[end] = text[byte];
                end += 1;
                byte += 1;
            }
            blocks[bits].ends[row] = end as u8;
            row += 1;
        }
        bits += 1;
    }
    blocks
}

static MODERN_WORDS: [WordBlock; 256] = word_blocks(false);
static LEGACY_WORDS: [WordBlock; 256] = word_blocks(true);

fn boolean_words(input: &BooleanArray, legacy: bool) -> Result<ArrayRef> {
    let Some(capacity) = input
        .len()
        .checked_mul(5)
        .and_then(|length| length.checked_sub(input.values().count_set_bits()))
        .filter(|length| *length <= i32::MAX as usize)
    else {
        return exec_err!("Boolean string batch exceeds Arrow UTF-8 offset capacity");
    };
    let blocks = if legacy { &LEGACY_WORDS } else { &MODERN_WORDS };
    let mut bytes = Vec::with_capacity(capacity);
    let mut offsets = Vec::with_capacity(input.len() + 1);
    offsets.push(0_i32);
    let mut append = |bits: u8, rows: usize| {
        let block = &blocks[bits as usize];
        let base = bytes.len() as i32;
        offsets.extend(block.ends[..rows].iter().map(|end| base + i32::from(*end)));
        bytes.extend_from_slice(&block.bytes[..usize::from(block.ends[rows - 1])]);
    };
    // Eight bitmap values select one byte block; NULL payload bytes stay masked by validity.
    let chunks = input.values().bit_chunks();
    for bits in chunks.iter() {
        for shift in (0..64).step_by(8) {
            append((bits >> shift) as u8, 8);
        }
    }
    let bits = chunks.remainder_bits();
    for shift in (0..chunks.remainder_len()).step_by(8) {
        append(
            (bits >> shift) as u8,
            (chunks.remainder_len() - shift).min(8),
        );
    }
    Ok(Arc::new(StringArray::new(
        OffsetBuffer::new(offsets.into()),
        bytes.into(),
        input.nulls().cloned(),
    )))
}

fn word(value: bool, legacy: bool, length: i32) -> Cow<'static, str> {
    let text = match (value, legacy) {
        (true, true) => "true",
        (false, true) => "false",
        (true, false) => "TRUE",
        (false, false) => "FALSE",
    };
    if legacy {
        return Cow::Borrowed(text);
    }
    let width = length.unsigned_abs() as usize;
    if length < 0 && width < i32::MAX as usize && width > text.len() {
        let mut padded = String::with_capacity(width);
        padded.push_str(text);
        padded.extend(std::iter::repeat_n(' ', width - text.len()));
        Cow::Owned(padded)
    } else {
        Cow::Borrowed(&text[..text.len().min(width)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn every_bitmap_word_and_unaligned_chunk_tail_keeps_its_values_and_validity() {
        let values: Vec<_> = (0..256)
            .flat_map(|bits| (0..8).map(move |bit| Some(bits & (1 << bit) != 0)))
            .collect();
        for nullable in [false, true] {
            let input = BooleanArray::from(
                values
                    .iter()
                    .enumerate()
                    .map(|(row, value)| {
                        if nullable && row % 7 == 0 {
                            None
                        } else {
                            *value
                        }
                    })
                    .collect::<Vec<_>>(),
            );
            for legacy in [false, true] {
                for start in 0..9 {
                    for length in [
                        0,
                        1,
                        7,
                        8,
                        9,
                        31,
                        63,
                        64,
                        65,
                        129,
                        1023,
                        2039,
                        input.len() - start,
                    ] {
                        let source = input.slice(start, length);
                        let actual = boolean_words(&source, legacy).unwrap();
                        let actual = actual.as_any().downcast_ref::<StringArray>().unwrap();
                        assert_eq!(actual.len(), source.len());
                        for (actual, value) in actual.iter().zip(source.iter()) {
                            assert_eq!(actual, expected(value, legacy, i32::MAX).as_deref());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn spellings_widths_scalars_and_sliced_nulls_match_cast_modes() {
        for legacy in [false, true] {
            for length in [1, 2, 3, 4, 5, 8, 32, i32::MAX] {
                for length in [length, -length] {
                    let source =
                        BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]);
                    for input in [
                        source.slice(1, 3),
                        source.slice(1, 0),
                        BooleanArray::from(vec![None; 3]),
                    ] {
                        let result = evaluate(
                            ColumnarValue::Array(Arc::new(input.clone())),
                            legacy,
                            length,
                        );
                        let result = result.into_array(input.len()).unwrap();
                        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
                        assert_eq!(result.len(), input.len());
                        for (actual, value) in result.iter().zip(input.iter()) {
                            assert_eq!(actual, expected(value, legacy, length).as_deref());
                        }
                    }
                    for value in [Some(true), Some(false), None] {
                        let result = evaluate(
                            ColumnarValue::Scalar(ScalarValue::Boolean(value)),
                            legacy,
                            length,
                        );
                        assert!(
                            matches!(result, ColumnarValue::Scalar(ScalarValue::Utf8(actual))
                            if actual == expected(value, legacy, length))
                        );
                    }
                }
            }
        }
    }

    fn expected(value: Option<bool>, legacy: bool, length: i32) -> Option<String> {
        value.map(|value| {
            let mut text = value.to_string();
            if !legacy {
                text = text.to_uppercase();
                let width = length.unsigned_abs() as usize;
                text.truncate(width);
                if length < 0 && width < i32::MAX as usize {
                    text = format!("{text:width$}");
                }
            }
            text
        })
    }

    fn evaluate(input: ColumnarValue, legacy: bool, length: i32) -> ColumnarValue {
        function()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    input,
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(legacy))),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(length))),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("v", DataType::Boolean, true)),
                    Arc::new(Field::new("legacy", DataType::Boolean, false)),
                    Arc::new(Field::new("length", DataType::Int32, false)),
                ],
                number_rows: 3,
                return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap()
    }
}
