//! Consume padding/overlay UTF-16 intermediates before their final UTF-8 encoding.
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Int32Array, Int64Array, NullBufferBuilder, StringArray,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

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
            string("a"),
            number(2),
            string("x"),
            string("?"),
            string("Z"),
        ];
        assert!(
            evaluate(26, &args, &[], 1).is_none(),
            "replacement of growing padding retains exact evaluation"
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
}

impl<'a> Span<'a> {
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
    Column(&'a StringArray, Option<JavaStringCacheColumn<'a>>),
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
                let cache = match cache {
                    Some(ColumnarValue::Array(array))
                        if array.data_type() != &arrow::datatypes::DataType::Null =>
                    {
                        Some(JavaStringCacheColumn::new(array))
                    }
                    _ => None,
                };
                Self::Column(value.as_any().downcast_ref()?, cache)
            }
            _ => return None,
        })
    }

    fn value(&self, row: usize) -> Option<Span<'_>> {
        match self {
            Self::Scalar(_, Some(units)) => Some(Span::Utf16(units)),
            Self::Scalar(value, None) => value.map(Span::Utf8),
            Self::Column(value, _) if value.is_null(row) => None,
            Self::Column(value, cache) => Some(
                cache
                    .as_ref()
                    .and_then(|cache| cache.value(row))
                    .map(Span::Utf16)
                    .unwrap_or_else(|| Span::Utf8(value.value(row))),
            ),
        }
    }
}

enum Integers<'a> {
    Scalar(Option<i64>),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
}

impl<'a> Integers<'a> {
    fn new(value: &'a ColumnarValue) -> Option<Self> {
        Some(match value {
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => Self::Scalar(value.map(i64::from)),
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Self::Scalar(*value),
            ColumnarValue::Scalar(ScalarValue::Null) => Self::Scalar(None),
            ColumnarValue::Array(array)
                if array.data_type() == &arrow::datatypes::DataType::Int32 =>
            {
                Self::I32(array.as_any().downcast_ref()?)
            }
            ColumnarValue::Array(array) => Self::I64(array.as_any().downcast_ref()?),
            _ => return None,
        })
    }
    fn value(&self, row: usize) -> Option<i64> {
        match self {
            Self::Scalar(value) => *value,
            Self::I32(value) => (!value.is_null(row)).then(|| i64::from(value.value(row))),
            Self::I64(value) => (!value.is_null(row)).then(|| value.value(row)),
        }
    }
}

/// None retains the exact evaluator, including growing replacements and overflowing overlay indexes.
pub(crate) fn evaluate(
    operation: i64,
    args: &[ColumnarValue],
    caches: &[ColumnarValue],
    rows: usize,
) -> Option<ArrayRef> {
    if !(20..=28).contains(&operation) {
        return None;
    }
    let producer = (operation - 20) % 3;
    let consumer = (operation - 20) / 3;
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
    for row in 0..rows {
        let values = source
            .value(row)
            .zip(pattern.value(row))
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
                            return None;
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
                let size = source.len();
                if number <= 0 || number as u64 > size as u64 {
                    Some(Text::single(source))
                } else {
                    let start = number as i32;
                    let length = length as i32;
                    let end = if length > 0 && i64::from(start.wrapping_add(length)) <= size as i64
                    {
                        let offset = start.wrapping_sub(1).wrapping_add(length);
                        if offset < 0 || offset as usize > size {
                            return None;
                        }
                        offset as usize
                    } else {
                        size
                    };
                    let prefix = source.split(start as usize - 1).0;
                    let suffix = source.split(end).1;
                    Some(Text([prefix[0], prefix[1], pattern, suffix[0], suffix[1]]))
                }
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
        let limit = text.0.iter().try_fold(self.bytes.len(), |size, span| {
            size.checked_add(match span {
                Span::Utf8(s) => s.len(),
                Span::Utf16(s) => s.len(),
                Span::Unit(_) => 4,
            })
        })?;
        if limit > i32::MAX as usize {
            return None;
        }
        self.bytes.reserve(limit - self.bytes.len());
        for span in text.0 {
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
