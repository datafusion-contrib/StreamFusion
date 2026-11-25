mod word_break;

use std::sync::Arc;
use word_break::WordBreak;

use super::byte_string_text::{flink_char_width, utf8_view, TextColumn, TextValue};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, ListArray, ListBuilder, StringArray,
    StructArray, UInt16Builder,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_datafusion_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

#[derive(Debug, PartialEq, Eq, Hash)]
struct CaseMapping {
    upper: bool,
    locale: u64,
    mappings: Arc<[(u32, u8, u64)]>,
    word_break: Option<Arc<WordBreak>>,
}

const ABOVE: u8 = 1;
const COMBINING: u8 = 2;
const SOFT_DOTTED: u8 = 4;
const CASED: u8 = 8;
const STARTS_UPPERCASE: u8 = 16;
const DOT_ABOVE: u32 = 0x0307;
const CAPITAL_SIGMA: u32 = 0x03a3;

impl CaseMapping {
    fn from_words(
        upper: bool,
        mut words: impl ExactSizeIterator<Item = u64>,
        word_break: Option<Arc<WordBreak>>,
    ) -> Option<Self> {
        let locale = words.next()?;
        if locale > 2 || words.len() % 2 != 0 {
            return None;
        }
        let mappings = std::iter::from_fn(|| {
            let flags = words.next()?;
            Some((
                (flags & ((1 << 21) - 1)) as u32,
                (flags >> 21) as u8,
                words.next()?,
            ))
        })
        .collect();
        Some(Self {
            upper,
            locale,
            mappings,
            word_break,
        })
    }

    #[cfg(test)]
    fn evaluate(&self, arrays: &[ArrayRef], caches: &[ArrayRef]) -> Option<ArrayRef> {
        let [source] = arrays else { return None };
        let cache = caches
            .first()
            .filter(|cache| cache.data_type() != &DataType::Null)
            .map(JavaStringCacheColumn::new);
        self.evaluate_output(source, None, cache.as_ref(), None, false, false)
    }

    fn evaluate_output(
        &self,
        source: &ArrayRef,
        parent: Option<&NullBuffer>,
        cache: Option<&JavaStringCacheColumn<'_>>,
        decoded_rows: Option<&BooleanArray>,
        represented: bool,
        literal: bool,
    ) -> Option<ArrayRef> {
        let text = TextColumn::new(source).ok()?;
        let binary = source.data_type() == &DataType::Binary;
        if (cache.is_none() || self.locale != 1)
            && parent.is_none()
            && decoded_rows.is_none()
            && !literal
        {
            let ascii = match &text {
                TextColumn::Utf8(text) if text.is_ascii() => Some(*text),
                TextColumn::Validated(text) if text.is_ascii() => Some(text),
                _ => None,
            };
            if let Some((values, caches)) =
                ascii.and_then(|ascii| ascii_case_values(ascii, self.upper, cache))
            {
                let values: ArrayRef = if binary {
                    let (offsets, values, nulls) = values.into_parts();
                    Arc::new(BinaryArray::new(offsets, values, nulls))
                } else {
                    Arc::new(values)
                };
                return if represented {
                    super::collection_string::represented_values(values, caches).ok()
                } else {
                    Some(values)
                };
            }
        }
        let mut output = BinaryBuilder::with_capacity(source.len(), 0);
        let value_type = super::collection_string::represented_type();
        let DataType::Struct(fields) = value_type else {
            unreachable!()
        };
        let DataType::List(unit) = fields[1].data_type() else {
            unreachable!()
        };
        let mut output_caches =
            represented.then(|| ListBuilder::new(UInt16Builder::new()).with_field(unit.clone()));
        let mut encoded = String::new();
        let mut decoded = String::new();
        let mut binary_case = Vec::new();
        let mut value = Vec::new();
        let mut context = Vec::new();
        let mut break_text = Vec::new();
        for row in 0..source.len() {
            let input = if parent.is_some_and(|parent| parent.is_null(row)) {
                None
            } else {
                text.get(row)
            };
            let Some(input) = input else {
                output.append_null();
                if let Some(caches) = &mut output_caches {
                    caches.append(false);
                }
                continue;
            };
            encoded.clear();
            value.clear();
            let original = cache.and_then(|cache| cache.value(row));
            let bytes = input.as_bytes();
            let requires_java = match input {
                TextValue::Utf8(value) => !value.is_ascii(),
                TextValue::Malformed(bytes) => bytes.iter().any(|&byte| flink_char_width(byte) > 1),
            };
            let cached = original.is_some()
                || requires_java
                || literal
                || decoded_rows.is_some_and(|rows| rows.value(row));
            if let Some(units) = original {
                self.map_text(
                    char::decode_utf16(units.iter().copied()).map(|point| {
                        point.map_or_else(|err| err.unpaired_surrogate() as u32, u32::from)
                    }),
                    &mut value,
                    &mut context,
                    &mut break_text,
                )?;
            } else if cached {
                let input = match input {
                    TextValue::Utf8(input) => input,
                    TextValue::Malformed(bytes) => {
                        decoded.clear();
                        super::scalar::append_java_utf8(bytes, &mut decoded);
                        &decoded
                    }
                };
                self.map_text(
                    input.chars().map(u32::from),
                    &mut value,
                    &mut context,
                    &mut break_text,
                )?;
            }
            let result = if cached {
                encoded.extend(
                    char::decode_utf16(value.iter().copied()).map(|point| point.unwrap_or('?')),
                );
                encoded.as_bytes()
            } else {
                binary_case.clear();
                binary_case.extend_from_slice(bytes);
                if self.upper {
                    binary_case.make_ascii_uppercase();
                } else {
                    binary_case.make_ascii_lowercase();
                }
                &binary_case
            };
            super::check_string_capacity(output.values_slice().len(), result.len()).ok()?;
            output.append_value(result);
            if let Some(caches) = &mut output_caches {
                if cached {
                    caches.values().append_slice(&value);
                }
                caches.append(cached);
            }
        }

        let output = output.finish();
        let output: ArrayRef = if binary {
            Arc::new(output)
        } else {
            Arc::new(utf8_view(&output)?)
        };
        if let Some(mut caches) = output_caches {
            super::collection_string::represented_values(output, Some(caches.finish())).ok()
        } else {
            Some(output)
        }
    }

    fn map_text(
        &self,
        input: impl Iterator<Item = u32> + Clone,
        out: &mut Vec<u16>,
        context: &mut Vec<u32>,
        break_text: &mut Vec<u16>,
    ) -> Option<()> {
        let has_sigma = !self.upper && input.clone().any(|point| point == CAPITAL_SIGMA);
        let contextual = match (self.locale, self.upper) {
            (1, false) | (2, true) => input.clone().any(|point| point == DOT_ABOVE),
            (2, false) => input
                .clone()
                .any(|point| matches!(point, 0x49 | 0x4a | 0x012e)),
            _ => false,
        };
        if !has_sigma && !contextual {
            for point in input {
                self.map_char(point, out);
            }
            return Some(());
        }

        context.clear();
        context.extend(input);
        if has_sigma {
            let word_break = self.word_break.as_ref()?;
            break_text.clear();
            for &point in context.iter() {
                if point <= 0xffff {
                    break_text.push(point as u16);
                } else {
                    let value = char::from_u32(point).expect("supplementary Unicode point");
                    break_text.extend_from_slice(value.encode_utf16(&mut [0; 2]));
                }
            }
            let mut unit = 0;
            for (position, &point) in context.iter().enumerate() {
                if point == CAPITAL_SIGMA
                    && word_break
                        .final_sigma(break_text, unit, |point| self.entry(point).0 & CASED != 0)
                {
                    out.push(0x03c2);
                } else if contextual {
                    self.map_context(point, &context[..position], &context[position + 1..], out);
                } else {
                    self.map_char(point, out);
                }
                unit += if point > 0xffff { 2 } else { 1 };
            }
        } else {
            // OpenJDK copies the unchanged prefix before entering Lithuanian special casing.
            let first_change = if self.locale == 2 && self.upper {
                context
                    .iter()
                    .position(|&point| {
                        point > 0xffff || (0xd800..=0xdfff).contains(&point) || {
                            let (properties, mapped) = self.entry(point);
                            mapped != 0 || properties & STARTS_UPPERCASE != 0
                        }
                    })
                    .unwrap_or(context.len())
            } else {
                0
            };
            for (offset, &point) in context.iter().enumerate() {
                if offset >= first_change {
                    self.map_context(point, &context[..offset], &context[offset + 1..], out);
                } else {
                    self.map_char(point, out);
                }
            }
        }
        Some(())
    }

    fn entry(&self, point: u32) -> (u8, u64) {
        self.mappings
            .binary_search_by_key(&point, |&(key, _, _)| key)
            .map_or((0, 0), |i| (self.mappings[i].1, self.mappings[i].2))
    }

    fn map_char(&self, point: u32, out: &mut Vec<u16>) {
        if point < 128 && (self.locale != 1 || point != if self.upper { 0x69 } else { 0x49 }) {
            let byte = point as u8;
            out.push(u16::from(if self.upper {
                byte.to_ascii_uppercase()
            } else {
                byte.to_ascii_lowercase()
            }));
            return;
        }
        let (_, units) = self.entry(point);
        if units != 0 {
            out.extend(
                (0..4)
                    .map(|i| (units >> (16 * i)) as u16)
                    .take_while(|unit| *unit != 0),
            );
        } else if point <= 0xffff {
            out.push(point as u16);
        } else {
            out.extend_from_slice(
                char::from_u32(point)
                    .expect("supplementary Unicode point")
                    .encode_utf16(&mut [0; 2]),
            );
        }
    }

    fn map_context(&self, point: u32, before: &[u32], after: &[u32], out: &mut Vec<u16>) {
        match (self.locale, self.upper, point) {
            (1, false, 0x49) => {
                let terminator = after
                    .iter()
                    .copied()
                    .find(|&next| next == DOT_ABOVE || self.entry(next).0 & COMBINING == 0);
                if terminator == Some(DOT_ABOVE) {
                    out.push(u16::from(b'i'));
                    return;
                }
            }
            (1, false, DOT_ABOVE) => {
                if before
                    .iter()
                    .rev()
                    .copied()
                    .find(|&prev| self.entry(prev).0 & COMBINING == 0)
                    == Some(0x49)
                {
                    return;
                }
            }
            (2, true, DOT_ABOVE) => {
                if before
                    .iter()
                    .rev()
                    .copied()
                    .find(|&prev| self.entry(prev).0 & COMBINING == 0)
                    .is_some_and(|prev| self.entry(prev).0 & SOFT_DOTTED != 0)
                {
                    return;
                }
            }
            (2, false, 0x49 | 0x4a | 0x012e) => {
                self.map_char(point, out);
                if after
                    .iter()
                    .copied()
                    .take_while(|&next| self.entry(next).0 & (ABOVE | COMBINING) != 0)
                    .any(|next| self.entry(next).0 & ABOVE != 0)
                {
                    out.push(DOT_ABOVE as u16);
                }
                return;
            }
            _ => {}
        }
        self.map_char(point, out);
    }
}

fn ascii_case_values(
    source: &StringArray,
    upper: bool,
    cache: Option<&JavaStringCacheColumn<'_>>,
) -> Option<(StringArray, Option<ListArray>)> {
    let offsets = source.value_offsets();
    let start = offsets[0];
    let end = offsets[source.len()];
    let mut values = source.value_data()[start as usize..end as usize].to_vec();
    let mut cached_units = 0;
    if let Some(cache) = cache {
        for (row, offsets) in offsets.windows(2).enumerate() {
            let Some(units) = cache.value(row).filter(|_| source.is_valid(row)) else {
                continue;
            };
            let bytes = &mut values[(offsets[0] - start) as usize..(offsets[1] - start) as usize];
            if units.len() != bytes.len() || units.iter().any(|&unit| unit >= 128) {
                return None;
            }
            cached_units += units.len();
            for (byte, &unit) in bytes.iter_mut().zip(units) {
                *byte = unit as u8;
            }
        }
    }
    if upper {
        values.make_ascii_uppercase();
    } else {
        values.make_ascii_lowercase();
    }
    let offsets = if start == 0 {
        source.offsets().clone()
    } else {
        OffsetBuffer::new(
            offsets
                .iter()
                .map(|offset| offset - start)
                .collect::<Vec<_>>()
                .into(),
        )
    };
    let caches = cache.map(|cache| {
        let mut output =
            ListBuilder::with_capacity(UInt16Builder::with_capacity(cached_units), source.len())
                .with_field(Arc::new(Field::new("unit", DataType::UInt16, false)));
        let mut units = Vec::new();
        for (row, offsets) in offsets.windows(2).enumerate() {
            let warm = source.is_valid(row) && cache.value(row).is_some();
            if warm {
                units.clear();
                units.extend(
                    values[offsets[0] as usize..offsets[1] as usize]
                        .iter()
                        .map(|&byte| u16::from(byte)),
                );
                output.values().append_slice(&units);
            }
            output.append(warm);
        }
        output.finish()
    });
    Some((
        StringArray::new(offsets, values.into(), source.nulls().cloned()),
        caches,
    ))
}

pub(crate) fn represented_case(words: Option<&[i64]>) -> ScalarUDF {
    ScalarUDF::new_from_impl(RepresentedCase {
        mapping: words.and_then(|words| {
            let [upper, mappings, word_break, rest @ ..] = words else {
                return None;
            };
            let (mappings, rest) = rest.split_at_checked(*mappings as usize)?;
            let dictionary_rules = WordBreak::from_words(
                rest.get(..*word_break as usize)?
                    .iter()
                    .map(|&word| word as u64),
            );
            CaseMapping::from_words(
                *upper == 1,
                mappings.iter().map(|&word| word as u64),
                dictionary_rules.map(Arc::new),
            )
        }),
        signature: Signature::any(3, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RepresentedCase {
    mapping: Option<CaseMapping>,
    signature: Signature,
}

impl ScalarUDFImpl for RepresentedCase {
    fn name(&self) -> &str {
        "flink_represented_string_case"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let source =
            super::collection_string::represented_byte_type(&types[0]).unwrap_or(&types[0]);
        Ok(super::collection_string::represented_type_for(
            source.clone(),
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let literal = matches!(args.args[0], ColumnarValue::Scalar(_));
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| exec_datafusion_err!("Missing worker JDK case mappings"))?;
        let output =
            if super::collection_string::represented_byte_type(arrays[0].data_type()).is_some() {
                let value = arrays[0].as_any().downcast_ref::<StructArray>().unwrap();
                let cache = JavaStringCacheColumn::new(value.column(1));
                mapping.evaluate_output(
                    value.column(0),
                    value.nulls(),
                    Some(&cache),
                    None,
                    true,
                    false,
                )
            } else {
                let cache = (arrays[1].data_type() != &DataType::Null)
                    .then(|| JavaStringCacheColumn::new(&arrays[1]));
                let decoded = arrays[2]
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .filter(|rows| rows.true_count() != 0);
                mapping.evaluate_output(&arrays[0], None, cache.as_ref(), decoded, true, literal)
            };
        output
            .map(ColumnarValue::Array)
            .ok_or_else(|| exec_datafusion_err!("Cannot apply worker JDK case mappings"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamfusion_bridge::string_cache::string_cache_array;

    #[test]
    fn mixed_case_uses_worker_mappings_only_when_flink_decodes_the_text() {
        let upper = CaseMapping {
            upper: true,
            locale: 1,
            word_break: None,
            mappings: Arc::new([('i' as u32, 0, 0x0130), ('ß' as u32, 0, 0x0053_0053)]),
        };
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            Some("i"),
            Some("i"),
            Some("iß"),
            None,
        ]));
        let cache = string_cache_array([None, Some(&[b'i' as u16][..]), None, None]);
        assert_eq!(
            upper.evaluate(&[source], &[cache]).unwrap().as_ref(),
            &StringArray::from(vec![Some("I"), Some("İ"), Some("İSS"), None]),
        );
    }

    #[test]
    fn turkic_case_maps_dot_after_i_without_touching_the_intervening_marks() {
        let lower = CaseMapping {
            upper: false,
            locale: 1,
            word_break: None,
            mappings: Arc::new([
                ('I' as u32, 0, 0x0131),
                (0x0301, ABOVE, 0),
                (0x0307, ABOVE, 0),
                (0x0327, COMBINING, 0),
            ]),
        };
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            "I\u{0307}",
            "I\u{0327}\u{0307}",
            "I\u{0301}\u{0307}",
        ]));
        assert_eq!(
            lower.evaluate(&[source], &[]).unwrap().as_ref(),
            &StringArray::from(vec!["i", "i\u{0327}", "ı\u{0301}\u{0307}"]),
        );
    }

    #[test]
    fn final_sigma_retains_the_stock_word_break_iterator() {
        let lower = CaseMapping {
            upper: false,
            locale: 0,
            word_break: None,
            mappings: Arc::new([]),
        };
        let source: ArrayRef = Arc::new(StringArray::from(vec!["ΟΣ"]));
        assert!(lower.evaluate(&[source], &[]).is_none());
    }
}
