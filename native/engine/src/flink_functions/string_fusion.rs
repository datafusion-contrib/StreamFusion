//! Consume padding/overlay UTF-16 intermediates before their final UTF-8 encoding.
use super::byte_string_text::{TextColumn, TextValue};
use crate::expression_protocol::{certified_op, scalar_op};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanBuilder, Int16Array, Int32Array, Int64Array, Int8Array,
    ListBuilder, NullBufferBuilder, StringArray, StringBuilder, StructArray, UInt16Builder,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::fmt::Write;
use std::sync::Arc;
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

// The Java operator sets this bit when opening a Calc on an older String implementation.
const LEGACY_OVERLAY_INDEX_MESSAGE: i64 = 1 << 16;

pub(crate) fn literal_comparison(operation: i64, count: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(LiteralComparison {
        operation,
        count,
        signature: Signature::any(count * 2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct LiteralComparison {
    operation: i64,
    count: usize,
    signature: Signature,
}

impl ScalarUDFImpl for LiteralComparison {
    fn name(&self) -> &str {
        "flink_string_literal_comparison"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (operands, caches) = args.args.split_at(self.count);
        evaluate(self.operation, operands, caches, args.number_rows)
            .map(ColumnarValue::Array)
            .ok_or_else(|| exec_datafusion_err!("Invalid operands for native string comparison"))
    }
}

pub(crate) fn represented_edit(operation: i64, count: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(RepresentedEdit {
        operation: operation & !LEGACY_OVERLAY_INDEX_MESSAGE,
        legacy_overlay_index_message: operation & LEGACY_OVERLAY_INDEX_MESSAGE != 0,
        count,
        signature: Signature::any(count * 2, Volatility::Immutable),
    })
}

pub(crate) fn represented_comparison(operation: i64) -> ScalarUDF {
    ScalarUDF::new_from_impl(RepresentedComparison {
        negate: operation == scalar_op::NOT_EQUAL,
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RepresentedComparison {
    negate: bool,
    signature: Signature,
}

impl ScalarUDFImpl for RepresentedComparison {
    fn name(&self) -> &str {
        "flink_represented_string_comparison"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let invalid = || exec_datafusion_err!("Invalid operands for represented string comparison");
        let left = Strings::new(&args.args[0], None).ok_or_else(invalid)?;
        let right = Strings::new(&args.args[1], None).ok_or_else(invalid)?;
        let mut result = BooleanBuilder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            result.append_option(
                left.comparison_value(row)
                    .zip(right.comparison_value(row))
                    .map(|(left, right)| left.equal(right) ^ self.negate),
            );
        }
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }
}

struct ComparisonValue<'a> {
    text: TextValue<'a>,
    warm: bool,
    utf16: Option<&'a [u16]>,
}

impl ComparisonValue<'_> {
    fn equal(self, other: Self) -> bool {
        if !self.warm || !other.warm {
            return self.text.as_bytes() == other.text.as_bytes();
        }
        match (self.utf16, other.utf16) {
            (Some(left), Some(right)) => left == right,
            (Some(left), None) => matches!(other.text, TextValue::Utf8(right)
                if left.iter().copied().eq(right.encode_utf16())),
            (None, Some(right)) => matches!(self.text, TextValue::Utf8(left)
                if left.encode_utf16().eq(right.iter().copied())),
            (None, None) => self.text.as_bytes() == other.text.as_bytes(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RepresentedEdit {
    operation: i64,
    legacy_overlay_index_message: bool,
    count: usize,
    signature: Signature,
}

impl ScalarUDFImpl for RepresentedEdit {
    fn name(&self) -> &str {
        "flink_represented_string_edit"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(super::collection_string::represented_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (operands, caches) = args.args.split_at(self.count);
        let invalid = || exec_datafusion_err!("Invalid operands for native string edit");
        let overlay = self.operation == scalar_op::OVERLAY;
        let source = Strings::new(&operands[0], caches.first()).ok_or_else(invalid)?;
        if self.operation == scalar_op::REPLACE {
            let old = Strings::new(&operands[1], caches.get(1)).ok_or_else(invalid)?;
            let replacement = Strings::new(&operands[2], caches.get(2)).ok_or_else(invalid)?;
            return represented_replace(source, old, replacement, args.number_rows);
        }
        let pattern_index = if overlay { 1 } else { 2 };
        let pattern = Strings::new(&operands[pattern_index], caches.get(pattern_index))
            .ok_or_else(invalid)?;
        let number = Integers::new(&operands[if overlay { 2 } else { 1 }]).ok_or_else(invalid)?;
        let length = operands.get(3).map(Integers::new);
        let length = match length {
            Some(Some(length)) => Some(length),
            Some(None) => return Err(invalid()),
            None => None,
        };
        let mut output = JavaStringOutput::new(args.number_rows);
        let mut text = Vec::new();
        let mut source_decoded = String::new();
        let mut pattern_decoded = String::new();
        for row in 0..args.number_rows {
            let length = length
                .as_ref()
                .map_or(Some(None), |length| length.value(row).map(Some));
            let Some((((source, pattern), number), length)) = source
                .value(row, &mut source_decoded)
                .zip(pattern.value(row, &mut pattern_decoded))
                .zip(number.value(row))
                .zip(length)
            else {
                output.append(None)?;
                continue;
            };
            text.clear();
            if overlay {
                let length = length.unwrap_or_else(|| pattern.len() as i64);
                if let (Span::Utf16(source), Span::Utf16(pattern)) = (source, pattern) {
                    if let Some((prefix, suffix)) = overlay_offsets(
                        source.len(),
                        number,
                        length,
                        self.legacy_overlay_index_message,
                    )? {
                        text.extend_from_slice(&source[..prefix]);
                        text.extend_from_slice(pattern);
                        text.extend_from_slice(&source[suffix..]);
                        output.append(Some(&text))?;
                    } else {
                        output.append(Some(source))?;
                    }
                    continue;
                } else {
                    let text_spans = overlay_spans(
                        source,
                        pattern,
                        number,
                        length,
                        self.legacy_overlay_index_message,
                    )?;
                    if matches!((source, pattern), (Span::Utf8(_), Span::Utf8(_)))
                        && output.append_utf8_spans(&text_spans.0, &mut text)?
                    {
                        continue;
                    }
                    for span in text_spans.0 {
                        span.append_units(&mut text);
                    }
                }
            } else if number < 0 || pattern.len() == 0 {
                output.append(None)?;
                continue;
            } else {
                let size = number as usize;
                super::check_string_capacity(output.cache.values().len(), size)?;
                pad_units(
                    source,
                    pattern,
                    size,
                    self.operation == scalar_op::LPAD,
                    &mut text,
                );
            }
            output.append(Some(&text))?;
        }
        output.finish()
    }
}

struct JavaStringOutput {
    cache: ListBuilder<UInt16Builder>,
    bytes: StringBuilder,
    encoded: Vec<u8>,
}

impl JavaStringOutput {
    fn new(rows: usize) -> Self {
        let DataType::Struct(fields) = super::collection_string::represented_type() else {
            unreachable!()
        };
        let DataType::List(unit) = fields[1].data_type() else {
            unreachable!()
        };
        let capacity = rows.saturating_mul(32).min(1 << 20);
        Self {
            cache: ListBuilder::with_capacity(UInt16Builder::with_capacity(capacity), rows)
                .with_field(unit.clone()),
            bytes: StringBuilder::with_capacity(rows, capacity),
            encoded: Vec::new(),
        }
    }

    fn append(&mut self, value: Option<&[u16]>) -> Result<()> {
        let Some(value) = value else {
            self.cache.append(false);
            self.bytes.append_null();
            return Ok(());
        };
        super::check_string_capacity(self.cache.values().len(), value.len())?;
        self.encoded.clear();
        if value.iter().all(|&unit| unit < 128) {
            self.encoded.extend(value.iter().map(|&unit| unit as u8));
        } else {
            encode_java_utf16(value, &mut self.encoded);
        }
        // SAFETY: ASCII units above and the encoder both produce valid UTF-8.
        let encoded = unsafe { std::str::from_utf8_unchecked(&self.encoded) };
        super::check_string_capacity(self.bytes.values_slice().len(), encoded.len())?;
        self.bytes.append_value(encoded);
        self.cache.values().append_slice(value);
        self.cache.append(true);
        Ok(())
    }

    fn append_utf8_spans(&mut self, spans: &[Span<'_>], units: &mut Vec<u16>) -> Result<bool> {
        let length = spans.iter().try_fold(0usize, |length, span| match span {
            Span::Utf8(value) => length.checked_add(value.len()),
            _ => None,
        });
        let Some(length) = length else {
            return Ok(false);
        };
        super::check_string_capacity(self.bytes.values_slice().len(), length)?;
        for &span in spans {
            let Span::Utf8(value) = span else {
                unreachable!()
            };
            self.bytes.write_str(value)?;
            span.append_units(units);
        }
        self.bytes.append_value("");
        // The completed byte buffer bounds the number of UTF-16 units.
        self.cache.values().append_slice(units);
        self.cache.append(true);
        Ok(true)
    }

    fn finish(mut self) -> Result<ColumnarValue> {
        super::collection_string::represented_values(
            Arc::new(self.bytes.finish()),
            Some(self.cache.finish()),
        )
        .map(ColumnarValue::Array)
    }
}

fn encode_java_utf16(mut units: &[u16], encoded: &mut Vec<u8>) {
    encoded.reserve(units.len().min(1 << 20) * 3);
    while let Some((&unit, remaining)) = units.split_first() {
        units = remaining;
        match unit {
            0..=0x7f => encoded.push(unit as u8),
            0x80..=0x7ff => {
                encoded.extend_from_slice(&[(0xc0 | unit >> 6) as u8, (0x80 | unit & 0x3f) as u8]);
            }
            0xd800..=0xdbff
                if units
                    .first()
                    .is_some_and(|low| (0xdc00..=0xdfff).contains(low)) =>
            {
                let point =
                    0x10000 + (u32::from(unit - 0xd800) << 10) + u32::from(units[0] - 0xdc00);
                units = &units[1..];
                encoded.extend_from_slice(&[
                    (0xf0 | point >> 18) as u8,
                    (0x80 | (point >> 12) & 0x3f) as u8,
                    (0x80 | (point >> 6) & 0x3f) as u8,
                    (0x80 | point & 0x3f) as u8,
                ]);
            }
            0xd800..=0xdfff => encoded.push(b'?'),
            _ => encoded.extend_from_slice(&[
                (0xe0 | unit >> 12) as u8,
                (0x80 | (unit >> 6) & 0x3f) as u8,
                (0x80 | unit & 0x3f) as u8,
            ]),
        }
    }
}

fn represented_replace(
    source: Strings<'_>,
    old: Strings<'_>,
    replacement: Strings<'_>,
    rows: usize,
) -> Result<ColumnarValue> {
    let mut result = JavaStringOutput::new(rows);
    let mut decoded = [String::new(), String::new(), String::new()];
    let mut input = [Vec::new(), Vec::new(), Vec::new()];
    let mut text = Vec::new();
    for row in 0..rows {
        let [source_decoded, old_decoded, new_decoded] = &mut decoded;
        let values = source
            .value(row, source_decoded)
            .zip(old.value(row, old_decoded))
            .zip(replacement.value(row, new_decoded));
        let Some(((source, old), replacement)) = values else {
            result.append(None)?;
            continue;
        };
        for (units, value) in input.iter_mut().zip([source, old, replacement]) {
            units.clear();
            value.append_units(units);
        }
        let [source, old, replacement] = &input;
        text.clear();
        if old.is_empty() {
            for &unit in source {
                super::check_string_capacity(text.len(), replacement.len() + 1)?;
                text.extend_from_slice(replacement);
                text.push(unit);
            }
            super::check_string_capacity(text.len(), replacement.len())?;
            text.extend_from_slice(replacement);
        } else {
            let mut rest = source.as_slice();
            while let Some(position) = rest
                .windows(old.len())
                .position(|candidate| candidate == old)
            {
                super::check_string_capacity(text.len(), position + replacement.len())?;
                text.extend_from_slice(&rest[..position]);
                text.extend_from_slice(replacement);
                rest = &rest[position + old.len()..];
            }
            super::check_string_capacity(text.len(), rest.len())?;
            text.extend_from_slice(rest);
        }
        result.append(Some(&text))?;
    }
    result.finish()
}

#[derive(Clone, Copy)]
enum Span<'a> {
    Utf8(&'a str),
    Utf16(&'a [u16]),
    Unit(u16),
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{new_null_array, BooleanArray};
    use arrow::datatypes::DataType;
    use streamfusion_bridge::string_cache::string_cache_array;

    fn string(value: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.into())))
    }
    fn number(value: i64) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value)))
    }

    #[test]
    fn edit_encoding_replaces_only_unpaired_surrogates() {
        let mut units: Vec<u16> = (0..=u16::MAX).collect();
        units.extend([0xd800, 0xdc00, 0xdbff, 0xdfff, 0xd800, b'?' as u16, 0xdc00]);
        let expected: String = char::decode_utf16(units.iter().copied())
            .map(|unit| unit.unwrap_or('?'))
            .collect();
        let mut encoded = Vec::new();
        encode_java_utf16(&units, &mut encoded);
        assert_eq!(encoded, expected.as_bytes());
    }

    #[test]
    fn padding_consumers_distinguish_surrogates_before_encoding_with_cached_and_cold_inputs() {
        let values = [Some("\u{1f600}"), Some("?"), Some("a"), None];
        let sources = ColumnarValue::Array(Arc::new(StringArray::from(values.to_vec())));
        let units = [vec![0xd83d, 0xde00], vec![63], vec![97]];
        let cache = ColumnarValue::Array(string_cache_array(
            units.iter().map(|v| Some(v.as_slice())).chain([None]),
        ));
        for caches in [vec![], vec![cache]] {
            for kind in [0, 1] {
                let args = [sources.clone(), number(1), string("x"), string("?")];
                let result = evaluate(20 + kind, &args, &caches, 4).unwrap();
                assert_eq!(
                    result.as_ref(),
                    &BooleanArray::from(vec![Some(false), Some(true), Some(false), None])
                );
                let result = evaluate(23 + kind, &args, &caches, 4).unwrap();
                assert_eq!(
                    result.as_ref(),
                    &BooleanArray::from(vec![Some(true), Some(false), Some(true), None])
                );
                let args = [
                    sources.clone(),
                    number(1),
                    string("x"),
                    string("?"),
                    string("Z"),
                ];
                if !caches.is_empty() {
                    assert!(evaluate(26 + kind, &args, &caches, 4).is_none());
                    continue;
                }
                let result = evaluate(26 + kind, &args, &caches, 4).unwrap();
                assert_eq!(
                    result.as_ref(),
                    &StringArray::from(vec![Some("?"), Some("Z"), Some("a"), None])
                );
                result.to_data().validate_full().unwrap();
            }
        }
    }

    #[test]
    fn original_cache_controls_values_even_when_its_encoded_bytes_match_question_mark() {
        let args = [string("?"), number(1), string("x"), string("?")];
        let mut args = args.to_vec();
        args[0] = ColumnarValue::Array(Arc::new(StringArray::from(vec!["?"])));
        let cache = ColumnarValue::Array(string_cache_array([Some(&[0xd800][..])]));
        let result = evaluate(20, &args, &[cache], 1).unwrap();
        assert_eq!(result.as_ref(), &BooleanArray::from(vec![false]));
    }

    #[test]
    fn overlay_consumers_keep_low_surrogates_and_default_length() {
        for source in ["\u{1f600}", "X?", "abc", ""] {
            let args = [
                string(source),
                string("X"),
                number(1),
                number(1),
                string("X?"),
            ];
            let result = evaluate(22, &args, &[], 1).unwrap();
            assert_eq!(result.as_ref(), &BooleanArray::from(vec![source == "X?"]));
        }
        let args = [
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["\u{1f600}"]))),
            string("X"),
            number(1),
            string("?"),
            string("Z"),
        ];
        assert_eq!(
            evaluate(28, &args, &[], 1).unwrap().as_ref(),
            &StringArray::from(vec!["X?"])
        );
    }

    #[test]
    fn replacement_can_rejoin_adjacent_utf16_surrogates_without_normalizing_them() {
        let args = [
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["\u{1f600}\u{1f601}"]))),
            string(""),
            number(2),
            number(2),
            string("?"),
            string("Z"),
        ];
        let result = evaluate(28, &args, &[], 1).unwrap();
        assert_eq!(result.as_ref(), &StringArray::from(vec!["\u{1f601}"]));
    }

    #[test]
    fn nulls_slices_empty_batches_and_uncertified_parameters_are_explicit() {
        for index in 0..4 {
            let mut args = [string("abc"), number(1), string("x"), string("a")];
            args[index] = ColumnarValue::Scalar(ScalarValue::Null);
            assert_eq!(
                evaluate(20, &args, &[], 2).unwrap().as_ref(),
                &BooleanArray::from(vec![None, None])
            );
        }
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("skip"), None, Some("abc")]));
        let args = [
            ColumnarValue::Array(values.slice(1, 2)),
            number(0),
            string("x"),
            string(""),
        ];
        assert_eq!(
            evaluate(20, &args, &[], 2).unwrap().as_ref(),
            &BooleanArray::from(vec![None, Some(true)])
        );
        let args = [
            string("abc"),
            string("x"),
            number(1),
            number(i64::from(i32::MAX)),
            string("x"),
        ];
        assert!(
            evaluate(22, &args, &[], 1).is_none(),
            "overflow retains Flink's exception"
        );
        let args = [
            ColumnarValue::Array(new_null_array(&DataType::Utf8, 0)),
            number(1),
            string("x"),
            string("?"),
        ];
        assert_eq!(evaluate(20, &args, &[], 0).unwrap().len(), 0);
    }

    #[test]
    fn growing_padding_compares_repeated_utf16_units_without_encoding() {
        for cached in [false, true] {
            for (source, pattern, length, left, right) in [
                ("a", "xy", 6, "xyxyxa", "axyxyx"),
                ("a", "xyabc", 4, "xyaa", "axya"),
                ("", "xy", 3, "xyx", "xyx"),
                (
                    "\u{1f600}",
                    "\u{4e2d}",
                    4,
                    "\u{4e2d}\u{4e2d}\u{1f600}",
                    "\u{1f600}\u{4e2d}\u{4e2d}",
                ),
                ("a", "\u{1f600}", 4, "\u{1f600}?a", "a\u{1f600}?"),
            ] {
                let sources =
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![Some(source), None])));
                let patterns = ColumnarValue::Array(Arc::new(StringArray::from(vec![pattern; 2])));
                let source_units: Vec<_> = source.encode_utf16().collect();
                let pattern_units: Vec<_> = pattern.encode_utf16().collect();
                let caches = if cached {
                    vec![
                        ColumnarValue::Array(string_cache_array([
                            Some(source_units.as_slice()),
                            None,
                        ])),
                        ColumnarValue::Scalar(ScalarValue::Null),
                        ColumnarValue::Array(string_cache_array(
                            [Some(pattern_units.as_slice()); 2],
                        )),
                    ]
                } else {
                    vec![]
                };
                for (kind, target) in [(0, left), (1, right)] {
                    let split_pair = source == "a" && pattern == "\u{1f600}";
                    for unequal in [false, true] {
                        let args = [
                            sources.clone(),
                            number(length),
                            patterns.clone(),
                            string(target),
                        ];
                        let result =
                            evaluate(20 + kind + i64::from(unequal) * 3, &args, &caches, 2)
                                .unwrap();
                        assert_eq!(
                            result.as_ref(),
                            &BooleanArray::from(vec![Some(!split_pair ^ unequal), None])
                        );
                    }
                }
            }
        }
        // A surrogate at the end of a repeated pad can join the original cached source.
        for (kind, source, pattern) in [(0, 0xde00, 0xd83d), (1, 0xd83d, 0xde00)] {
            let args = [
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["?"]))),
                number(2),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["?"]))),
                string("\u{1f600}"),
            ];
            let caches = [
                ColumnarValue::Array(string_cache_array([Some(&[source][..])])),
                ColumnarValue::Scalar(ScalarValue::Null),
                ColumnarValue::Array(string_cache_array([Some(&[pattern][..])])),
            ];
            assert_eq!(
                evaluate(20 + kind, &args, &caches, 1).unwrap().as_ref(),
                &BooleanArray::from(vec![true])
            );
        }
        let args = [
            string("a"),
            number(i64::from(i32::MAX)),
            string("xy"),
            string("a"),
        ];
        assert_eq!(
            evaluate(20, &args, &[], 1).unwrap().as_ref(),
            &BooleanArray::from(vec![false])
        );
        for (target, expected) in [
            ("xyxyxa", true),
            ("xyxxya", false),
            ("xyxyya", false),
            ("zyxyxa", false),
            ("xyxyxb", false),
        ] {
            let args = [string("a"), number(6), string("xy"), string(target)];
            assert_eq!(
                evaluate(20, &args, &[], 1).unwrap().as_ref(),
                &BooleanArray::from(vec![expected])
            );
        }
    }

    #[test]
    fn growing_padding_replacement_copies_full_periods_and_preserves_split_surrogates() {
        for (source, pattern, length, left, right) in [
            ("a?", "xy?", 9, "xyZxyZxaZ", "aZxyZxyZx"),
            ("a?", "xy?", 3, "xaZ", "aZx"),
            ("", "?", 9, "ZZZZZZZZZ", "ZZZZZZZZZ"),
            (
                "a?",
                "\u{1f600}?",
                7,
                "\u{1f600}Z\u{1f600}aZ",
                "aZ\u{1f600}Z\u{1f600}",
            ),
            ("a?", "\u{1f600}?", 6, "\u{1f600}Z?aZ", "aZ\u{1f600}Z?"),
            (
                "\u{1f600}?",
                "\u{4e2d}?",
                6,
                "\u{4e2d}Z\u{4e2d}\u{1f600}Z",
                "\u{1f600}Z\u{4e2d}Z\u{4e2d}",
            ),
        ] {
            for cached in [false, true] {
                let caches = if cached {
                    let sources: Vec<_> = source.encode_utf16().collect();
                    let patterns: Vec<_> = pattern.encode_utf16().collect();
                    vec![
                        ColumnarValue::Array(string_cache_array([Some(sources.as_slice()), None])),
                        ColumnarValue::Scalar(ScalarValue::Null),
                        ColumnarValue::Array(string_cache_array([Some(patterns.as_slice()); 2])),
                    ]
                } else {
                    vec![]
                };
                let args = [
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![Some(source), None]))),
                    number(length),
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![pattern; 2]))),
                    string("?"),
                    string("Z"),
                ];
                for (operation, expected) in [(26, left), (27, right)] {
                    let result = evaluate(operation, &args, &caches, 2);
                    if cached && (!source.is_ascii() || !pattern.is_ascii()) {
                        assert!(
                            result.is_none(),
                            "Unicode caches retain their exact evaluator"
                        );
                    } else {
                        let result = result.unwrap();
                        assert_eq!(
                            result.as_ref(),
                            &StringArray::from(vec![Some(expected), None])
                        );
                        result.to_data().validate_full().unwrap();
                    }
                }
            }
        }
        let args = [
            string(""),
            number(i64::from(i32::MAX)),
            string("\u{4e2d}"),
            string("?"),
            string("Z"),
        ];
        assert!(
            evaluate(26, &args, &[], 1).is_none(),
            "Arrow offset overflow retains exact evaluation before allocating"
        );
    }
}

impl<'a> Span<'a> {
    fn append_units(self, output: &mut Vec<u16>) {
        match self {
            Self::Utf8(s) if s.is_ascii() => output.extend(s.bytes().map(u16::from)),
            Self::Utf8(s) => output.extend(s.encode_utf16()),
            Self::Utf16(units) => output.extend_from_slice(units),
            Self::Unit(unit) => output.push(unit),
        }
    }

    fn byte_capacity(self) -> usize {
        match self {
            Self::Utf8(s) => s.len(),
            Self::Utf16(s) => s.len(), // Replacement admits only ASCII Java caches.
            Self::Unit(_) => 4,
        }
    }

    fn starts_with_units(self, units: &[u16]) -> bool {
        match self {
            Self::Utf8(s) if s.is_ascii() => s
                .bytes()
                .map(u16::from)
                .take(units.len())
                .eq(units.iter().copied()),
            Self::Utf8(s) => s.encode_utf16().take(units.len()).eq(units.iter().copied()),
            Self::Utf16(s) => s.starts_with(units),
            Self::Unit(unit) => units.is_empty() || units == [unit],
        }
    }

    fn len(self) -> usize {
        match self {
            Self::Utf8(s) if s.is_ascii() => s.len(),
            Self::Utf8(s) => s.encode_utf16().count(),
            Self::Utf16(s) => s.len(),
            Self::Unit(_) => 1,
        }
    }

    fn split(self, at: usize) -> ([Self; 2], [Self; 2]) {
        let empty = Self::Utf8("");
        match self {
            Self::Utf16(s) => (
                [Self::Utf16(&s[..at]), empty],
                [Self::Utf16(&s[at..]), empty],
            ),
            Self::Utf8(s) if s.is_ascii() => {
                ([Self::Utf8(&s[..at]), empty], [Self::Utf8(&s[at..]), empty])
            }
            Self::Utf8(s) => {
                let mut units = 0;
                for (offset, ch) in s.char_indices() {
                    if units == at {
                        return (
                            [Self::Utf8(&s[..offset]), empty],
                            [Self::Utf8(&s[offset..]), empty],
                        );
                    }
                    if units + ch.len_utf16() > at {
                        let mut pair = [0; 2];
                        ch.encode_utf16(&mut pair);
                        return (
                            [Self::Utf8(&s[..offset]), Self::Unit(pair[0])],
                            [
                                Self::Unit(pair[1]),
                                Self::Utf8(&s[offset + ch.len_utf8()..]),
                            ],
                        );
                    }
                    units += ch.len_utf16();
                }
                ([self, empty], [empty, empty])
            }
            Self::Unit(_) if at == 0 => ([empty, empty], [self, empty]),
            Self::Unit(_) => ([self, empty], [empty, empty]),
        }
    }
}

fn growing_padding_equals(
    source: Span<'_>,
    pattern: Span<'_>,
    source_length: usize,
    length: usize,
    target: &[u16],
    left: bool,
) -> bool {
    if length != target.len() {
        return false;
    }
    let (padding, original) = if left {
        target.split_at(length - source_length)
    } else {
        let (original, padding) = target.split_at(source_length);
        (padding, original)
    };
    let period = pattern.len();
    // One matching period plus equal shifted slices proves all repetitions, including a partial last one.
    source.starts_with_units(original)
        && pattern.starts_with_units(&padding[..padding.len().min(period)])
        && (padding.len() <= period || padding[period..] == padding[..padding.len() - period])
}

struct Text<'a>([Span<'a>; 5]);

fn pad_units(
    source: Span<'_>,
    pattern: Span<'_>,
    length: usize,
    left: bool,
    output: &mut Vec<u16>,
) {
    let source_length = source.len();
    if length <= source_length {
        for span in source.split(length).0 {
            span.append_units(output);
        }
        return;
    }
    if !left {
        source.append_units(output);
    }
    let needed = length - source_length;
    let period = pattern.len();
    let full_units = needed / period * period;
    if full_units != 0 {
        let start = output.len();
        pattern.append_units(output);
        while output.len() - start < full_units {
            let copied = output.len() - start;
            let take = copied.min(full_units - copied);
            output.extend_from_within(start..start + take);
        }
    }
    for span in pattern.split(needed % period).0 {
        span.append_units(output);
    }
    if left {
        source.append_units(output);
    }
}

fn overlay_spans<'a>(
    source: Span<'a>,
    pattern: Span<'a>,
    start: i64,
    length: i64,
    legacy_index_message: bool,
) -> Result<Text<'a>> {
    let Some((prefix, suffix)) =
        overlay_offsets(source.len(), start, length, legacy_index_message)?
    else {
        return Ok(Text::single(source));
    };
    let prefix = source.split(prefix).0;
    let suffix = source.split(suffix).1;
    Ok(Text([prefix[0], prefix[1], pattern, suffix[0], suffix[1]]))
}

fn overlay_offsets(
    size: usize,
    start: i64,
    length: i64,
    legacy_index_message: bool,
) -> Result<Option<(usize, usize)>> {
    if start <= 0 || start as u64 > size as u64 {
        return Ok(None);
    }
    let start = start as i32;
    let length = length as i32;
    let end = if length > 0 && i64::from(start.wrapping_add(length)) <= size as i64 {
        let offset = start.wrapping_sub(1).wrapping_add(length);
        if offset < 0 || offset as usize > size {
            return Err(DataFusionError::External(Box::new(
                streamfusion_bridge::FlinkException::string_index_out_of_bounds(
                    offset,
                    size,
                    legacy_index_message,
                ),
            )));
        }
        offset as usize
    } else {
        size
    };
    Ok(Some((start as usize - 1, end)))
}

impl<'a> Text<'a> {
    fn single(value: Span<'a>) -> Self {
        Self([
            value,
            Span::Utf8(""),
            Span::Utf8(""),
            Span::Utf8(""),
            Span::Utf8(""),
        ])
    }

    fn equals(&self, bytes: &str, units: &[u16]) -> bool {
        if self.0.iter().all(|s| matches!(s, Span::Utf8(_))) {
            let mut remaining = bytes.as_bytes();
            for span in self.0 {
                if let Span::Utf8(s) = span {
                    if !remaining.starts_with(s.as_bytes()) {
                        return false;
                    }
                    remaining = &remaining[s.len()..];
                }
            }
            return remaining.is_empty();
        }
        let mut remaining = units;
        for span in self.0 {
            match span {
                Span::Utf16(s) => {
                    if !remaining.starts_with(s) {
                        return false;
                    }
                    remaining = &remaining[s.len()..];
                }
                Span::Unit(unit) => {
                    if remaining.first() != Some(&unit) {
                        return false;
                    }
                    remaining = &remaining[1..];
                }
                Span::Utf8(s) => {
                    for unit in s.encode_utf16() {
                        if remaining.first() != Some(&unit) {
                            return false;
                        }
                        remaining = &remaining[1..];
                    }
                }
            }
        }
        remaining.is_empty()
    }
}

enum Strings<'a> {
    Scalar(Option<&'a str>, Option<Vec<u16>>),
    Column(TextColumn<'a>, Option<JavaStringCacheColumn<'a>>),
    Represented(&'a StructArray, TextColumn<'a>, JavaStringCacheColumn<'a>),
}

impl<'a> Strings<'a> {
    fn new(value: &'a ColumnarValue, cache: Option<&'a ColumnarValue>) -> Option<Self> {
        Some(match value {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Self::Scalar(
                value.as_deref(),
                value
                    .as_ref()
                    .filter(|s| !s.is_ascii())
                    .map(|s| s.encode_utf16().collect()),
            ),
            ColumnarValue::Scalar(ScalarValue::Null) => Self::Scalar(None, None),
            ColumnarValue::Array(value) => {
                if super::collection_string::represented_byte_type(value.data_type()).is_some() {
                    let value = value.as_any().downcast_ref::<StructArray>()?;
                    return Some(Self::Represented(
                        value,
                        TextColumn::new(value.column(0)).ok()?,
                        JavaStringCacheColumn::new(value.column(1)),
                    ));
                }
                let cache = match cache {
                    Some(ColumnarValue::Array(array))
                        if array.data_type() != &arrow::datatypes::DataType::Null =>
                    {
                        Some(JavaStringCacheColumn::new(array))
                    }
                    _ => None,
                };
                Self::Column(TextColumn::new(value).ok()?, cache)
            }
            _ => return None,
        })
    }

    fn value<'b>(&'b self, row: usize, decoded: &'b mut String) -> Option<Span<'b>> {
        let value = self.comparison_value(row)?;
        if let Some(units) = value.utf16 {
            return Some(Span::Utf16(units));
        }
        match value.text {
            TextValue::Utf8(value) => Some(Span::Utf8(value)),
            TextValue::Malformed(bytes) => {
                decoded.clear();
                super::scalar::append_java_utf8(bytes, decoded);
                Some(Span::Utf8(decoded))
            }
        }
    }

    fn comparison_value(&self, row: usize) -> Option<ComparisonValue<'_>> {
        match self {
            Self::Scalar(value, cache) => Some(ComparisonValue {
                text: TextValue::Utf8((*value)?),
                warm: true,
                utf16: cache.as_deref(),
            }),
            Self::Column(value, cache) => {
                let utf16 = cache.as_ref().and_then(|cache| cache.value(row));
                Some(ComparisonValue {
                    text: value.get(row)?,
                    warm: utf16.is_some(),
                    utf16,
                })
            }
            Self::Represented(parent, value, cache) if parent.is_valid(row) => {
                let utf16 = cache.value(row);
                Some(ComparisonValue {
                    text: value.get(row)?,
                    warm: utf16.is_some(),
                    utf16,
                })
            }
            _ => None,
        }
    }
}

enum Integers<'a> {
    Scalar(Option<i64>),
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
}

impl<'a> Integers<'a> {
    fn new(value: &'a ColumnarValue) -> Option<Self> {
        Some(match value {
            ColumnarValue::Scalar(ScalarValue::Int8(value)) => Self::Scalar(value.map(i64::from)),
            ColumnarValue::Scalar(ScalarValue::Int16(value)) => Self::Scalar(value.map(i64::from)),
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => Self::Scalar(value.map(i64::from)),
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Self::Scalar(*value),
            ColumnarValue::Scalar(ScalarValue::Null) => Self::Scalar(None),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Int8 => Self::I8(array.as_any().downcast_ref()?),
                DataType::Int16 => Self::I16(array.as_any().downcast_ref()?),
                DataType::Int32 => Self::I32(array.as_any().downcast_ref()?),
                DataType::Int64 => Self::I64(array.as_any().downcast_ref()?),
                _ => return None,
            },
            _ => return None,
        })
    }
    fn value(&self, row: usize) -> Option<i64> {
        match self {
            Self::Scalar(value) => *value,
            Self::I8(value) => (!value.is_null(row)).then(|| i64::from(value.value(row))),
            Self::I16(value) => (!value.is_null(row)).then(|| i64::from(value.value(row))),
            Self::I32(value) => (!value.is_null(row)).then(|| i64::from(value.value(row))),
            Self::I64(value) => (!value.is_null(row)).then(|| value.value(row)),
        }
    }
}

/// None retains the exact evaluator, including unverified caches and overflowing overlay indexes.
pub(crate) fn evaluate(
    operation: i64,
    args: &[ColumnarValue],
    caches: &[ColumnarValue],
    rows: usize,
) -> Option<ArrayRef> {
    if !(certified_op::LPAD_EQUAL..=certified_op::OVERLAY_REPLACE).contains(&operation) {
        return None;
    }
    let producer = (operation - certified_op::LPAD_EQUAL) % 3;
    let consumer = (operation - certified_op::LPAD_EQUAL) / 3;
    let count = args.len().checked_sub(if consumer == 2 { 2 } else { 1 })?;
    if count != 3 && !(producer == 2 && count == 4) {
        return None;
    }
    let source = Strings::new(&args[0], caches.first())?;
    let pattern_index = if producer == 2 { 1 } else { 2 };
    let pattern = Strings::new(&args[pattern_index], caches.get(pattern_index))?;
    let number = Integers::new(&args[if producer == 2 { 2 } else { 1 }])?;
    let length = if count == 4 {
        Some(Integers::new(&args[3])?)
    } else {
        None
    };
    let literal = |index| match &args[index] {
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Some(value.as_deref()),
        ColumnarValue::Scalar(ScalarValue::Null) => Some(None),
        _ => None,
    };
    let old = literal(count)?;
    let replacement = if consumer == 2 {
        literal(count + 1)?
    } else {
        None
    };
    if consumer == 2
        && (old?.len() != 1
            || !old?.is_ascii()
            || replacement?.len() != 1
            || !replacement?.is_ascii())
    {
        return None;
    }
    let target_units: Vec<_> = old.unwrap_or("").encode_utf16().collect();
    let mut booleans = BooleanBuilder::with_capacity(rows);
    let mut output = Output::new(rows);
    let mut source_decoded = String::new();
    let mut pattern_decoded = String::new();
    for row in 0..rows {
        let values = source
            .value(row, &mut source_decoded)
            .zip(pattern.value(row, &mut pattern_decoded))
            .zip(number.value(row));
        let value = if let Some(((source, pattern), number)) = values {
            // Cached Unicode encoding has not beaten the retained Flink evaluator.
            if consumer == 2 && [source, pattern].iter().any(|span|
                matches!(span, Span::Utf16(units) if units.iter().any(|unit| *unit >= 128))) {
                return None;
            }
            if producer != 2 {
                if number < 0 || pattern.len() == 0 {
                    None
                } else {
                    let length = usize::try_from(number).ok()?;
                    let source_length = source.len();
                    if length > source_length {
                        if consumer == 2 {
                            output.padding_replace(
                                source,
                                pattern,
                                length - source_length,
                                producer == 0,
                                old?.as_bytes()[0],
                                replacement?.as_bytes()[0],
                            )?;
                            continue;
                        }
                        booleans.append_option(old.map(|_| {
                            growing_padding_equals(
                                source,
                                pattern,
                                source_length,
                                length,
                                &target_units,
                                producer == 0,
                            ) ^ (consumer == 1)
                        }));
                        continue;
                    }
                    let prefix = source.split(length).0;
                    Some(Text([
                        prefix[0],
                        prefix[1],
                        Span::Utf8(""),
                        Span::Utf8(""),
                        Span::Utf8(""),
                    ]))
                }
            } else if let Some(length) = length
                .as_ref()
                .map_or_else(|| Some(pattern.len() as i64), |length| length.value(row))
            {
                Some(overlay_spans(source, pattern, number, length, false).ok()?)
            } else {
                None
            }
        } else {
            None
        };
        if consumer == 2 {
            if let Some(value) = value {
                output.replace(&value, old?.as_bytes()[0], replacement?.as_bytes()[0])?;
            } else {
                output.null();
            }
        } else {
            booleans.append_option(
                value
                    .filter(|_| old.is_some())
                    .map(|value| value.equals(old.unwrap(), &target_units) ^ (consumer == 1)),
            );
        }
    }
    Some(if consumer == 2 {
        Arc::new(output.finish())
    } else {
        Arc::new(booleans.finish())
    })
}

struct Output {
    bytes: Vec<u8>,
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    high: Option<u16>,
}

impl Output {
    fn new(rows: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        Self {
            bytes: Vec::new(),
            offsets,
            nulls: NullBufferBuilder::new(rows),
            high: None,
        }
    }
    fn null(&mut self) {
        self.nulls.append_null();
        self.offsets.push(self.bytes.len() as i32);
    }
    fn flush_high(&mut self) {
        if self.high.take().is_some() {
            self.bytes.push(b'?');
        }
    }
    fn unit(&mut self, unit: u16) {
        if (0xd800..=0xdbff).contains(&unit) {
            self.flush_high();
            self.high = Some(unit);
        } else {
            let ch = if (0xdc00..=0xdfff).contains(&unit) {
                self.high
                    .take()
                    .map(|high| {
                        char::from_u32(
                            0x10000 + (((high - 0xd800) as u32) << 10) + (unit - 0xdc00) as u32,
                        )
                        .unwrap()
                    })
                    .unwrap_or('?')
            } else {
                self.flush_high();
                char::from_u32(unit as u32).unwrap()
            };
            self.bytes
                .extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
        }
    }
    fn replace(&mut self, text: &Text, old: u8, new: u8) -> Option<()> {
        let capacity = text
            .0
            .iter()
            .try_fold(0usize, |size, span| size.checked_add(span.byte_capacity()))?;
        self.reserve(capacity)?;
        for span in text.0 {
            self.append_span(span, old, new);
        }
        self.finish_row()
    }
    fn reserve(&mut self, additional: usize) -> Option<()> {
        if self.bytes.len().checked_add(additional)? > i32::MAX as usize {
            return None;
        }
        self.bytes.reserve(additional);
        Some(())
    }
    #[inline(always)]
    fn append_span(&mut self, span: Span<'_>, old: u8, new: u8) {
        match span {
            Span::Utf8(s) => {
                if !s.is_empty() {
                    self.flush_high();
                }
                self.bytes
                    .extend(s.bytes().map(|byte| if byte == old { new } else { byte }));
            }
            Span::Utf16(units) => {
                debug_assert!(units.iter().all(|unit| *unit < 128));
                if !units.is_empty() {
                    self.flush_high();
                }
                self.bytes.extend(units.iter().map(|&unit| {
                    if unit == u16::from(old) {
                        new
                    } else {
                        unit as u8
                    }
                }));
            }
            Span::Unit(unit) => self.unit(unit),
        }
    }
    fn padding_replace(
        &mut self,
        source: Span<'_>,
        pattern: Span<'_>,
        units: usize,
        left: bool,
        old: u8,
        new: u8,
    ) -> Option<()> {
        let period = pattern.len();
        let repeats = units / period;
        let tail = pattern.split(units % period).0;
        let repeated_bytes = pattern.byte_capacity().checked_mul(repeats)?;
        let capacity = tail.iter().try_fold(
            source.byte_capacity().checked_add(repeated_bytes)?,
            |size, span| size.checked_add(span.byte_capacity()),
        )?;
        self.reserve(capacity)?;
        if !left {
            self.append_span(source, old, new);
        }
        if repeats != 0 {
            let start = self.bytes.len();
            self.append_span(pattern, old, new);
            // Map one whole period, then copy it in growing chunks into the final buffer.
            while self.bytes.len() - start < repeated_bytes {
                let copied = self.bytes.len() - start;
                let count = copied.min(repeated_bytes - copied);
                self.bytes.extend_from_within(start..start + count);
            }
        }
        for span in tail {
            self.append_span(span, old, new);
        }
        if left {
            self.append_span(source, old, new);
        }
        self.finish_row()
    }
    fn finish_row(&mut self) -> Option<()> {
        self.flush_high();
        self.offsets.push(i32::try_from(self.bytes.len()).ok()?);
        self.nulls.append_non_null();
        Some(())
    }
    fn finish(mut self) -> StringArray {
        StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(self.offsets)),
            Buffer::from(self.bytes),
            self.nulls.finish(),
        )
    }
}
