use super::{integer, number_output, text};
use crate::expression_protocol::certified_op;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use regex::bytes::{Regex, RegexBuilder};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) fn evaluate(op: i64, args: &[ArrayRef], target: &DataType) -> Option<ArrayRef> {
    let mut cache = HashMap::new();
    let mut likes = HashMap::new();
    let mut strings = Vec::with_capacity(args.first()?.len());
    let mut numbers = Vec::with_capacity(args[0].len());
    for row in 0..args[0].len() {
        if args.iter().any(|a| a.is_null(row)) {
            strings.push(None);
            numbers.push(None);
            continue;
        }
        let value = text(&args[0], row)?;
        let pattern = text(&args[1], row)?;
        let escape = if op >= certified_op::LIKE_WITH_ESCAPE && args.len() == 3 {
            Some(text(&args[2], row)?)
        } else {
            None
        };
        let key = (pattern, escape);
        if op == certified_op::LIKE_WITH_ESCAPE {
            if !likes.contains_key(&key) {
                likes.insert(key, Like::compile(pattern, escape)?);
            }
            numbers.push(Some(i64::from(likes.get(&key)?.matches(value))));
            strings.push(None);
            continue;
        }
        if !cache.contains_key(&key) {
            let source = if op >= certified_op::LIKE_WITH_ESCAPE {
                sql_pattern(pattern, escape, op == certified_op::SIMILAR)?
            } else {
                compatible_regex(pattern)?
            };
            cache.insert(key, RegexBuilder::new(&source).unicode(false).build().ok()?);
        }
        let regex: &Regex = cache.get(&key)?;
        let mut string = None;
        let first = regex.find(value.as_bytes());
        let number = match op {
            certified_op::REGEXP | certified_op::LIKE_WITH_ESCAPE | certified_op::SIMILAR => {
                Some(i64::from(first.is_some()))
            }
            certified_op::REGEXP_INSTR => Some(first.map_or(0, |m| m.start() as i64 + 1)),
            certified_op::REGEXP_COUNT | certified_op::REGEXP_REPLACE => {
                let replacement = if op == certified_op::REGEXP_REPLACE {
                    text(&args[2], row)?
                } else {
                    ""
                };
                let mut result = String::new();
                let mut previous = 0;
                let mut start = 0;
                let mut count = 0;
                // Java permits an empty match immediately after a nonempty match;
                // regex::find_iter suppresses it, so advance explicitly instead.
                while start <= value.len() {
                    let Some(found) = regex.find_at(value.as_bytes(), start) else {
                        break;
                    };
                    if op == certified_op::REGEXP_REPLACE {
                        result.push_str(&value[previous..found.start()]);
                        result.push_str(replacement);
                        previous = found.end();
                    }
                    count += 1;
                    start = found.end() + usize::from(found.start() == found.end());
                }
                if op == certified_op::REGEXP_REPLACE {
                    result.push_str(&value[previous..]);
                    string = Some(result);
                    None
                } else {
                    Some(count)
                }
            }
            certified_op::REGEXP_EXTRACT => {
                let index = if args.len() == 3 {
                    integer(&args[2], row)? as i32
                } else {
                    0
                };
                if index >= 0 {
                    string = regex.captures(value.as_bytes()).and_then(|m| {
                        m.get(index as usize)
                            .map(|m| value[m.start()..m.end()].to_owned())
                    });
                }
                None
            }
            certified_op::REGEXP_SUBSTR => {
                string = first.map(|m| value[m.start()..m.end()].to_owned());
                None
            }
            _ => return None,
        };
        strings.push(string);
        numbers.push(number);
    }
    if *target == DataType::Utf8 {
        Some(Arc::new(StringArray::from(strings)))
    } else {
        number_output(numbers, target)
    }
}

#[derive(Clone, Copy, PartialEq)]
enum LikeToken {
    Literal(u8),
    One,
    Many,
}

struct Like {
    tokens: Vec<LikeToken>,
    literal: Option<(Vec<u8>, bool, bool)>,
}
impl Like {
    fn compile(pattern: &str, escape: Option<&str>) -> Option<Self> {
        let escape = match escape {
            Some(s) if s.len() == 1 && s.as_bytes()[0] != 0 => Some(s.as_bytes()[0]),
            Some(_) => return None,
            None => None,
        };
        let mut tokens = Vec::new();
        let mut bytes = pattern.bytes();
        while let Some(byte) = bytes.next() {
            let token = if Some(byte) == escape {
                let next = bytes.next()?;
                if !b"_%".contains(&next) && Some(next) != escape {
                    return None;
                }
                LikeToken::Literal(next)
            } else {
                match byte {
                    b'%' => LikeToken::Many,
                    b'_' => LikeToken::One,
                    _ => LikeToken::Literal(byte),
                }
            };
            if token != LikeToken::Many || tokens.last() != Some(&LikeToken::Many) {
                tokens.push(token);
            }
        }
        let prefix = tokens.first() == Some(&LikeToken::Many);
        let suffix = tokens.last() == Some(&LikeToken::Many);
        let inner = &tokens[usize::from(prefix)
            ..tokens
                .len()
                .saturating_sub(usize::from(suffix && tokens.len() > usize::from(prefix)))];
        let literal = inner
            .iter()
            .map(|t| {
                if let LikeToken::Literal(byte) = t {
                    Some(*byte)
                } else {
                    None
                }
            })
            .collect::<Option<Vec<_>>>()
            .map(|literal| (literal, prefix, suffix));
        Some(Self { tokens, literal })
    }
    fn matches(&self, value: &str) -> bool {
        if let Some((literal, prefix, suffix)) = &self.literal {
            return match (*prefix, *suffix) {
                (false, false) => value.as_bytes() == literal,
                (false, true) => value.as_bytes().starts_with(literal),
                (true, false) => value.as_bytes().ends_with(literal),
                (true, true) => memchr::memmem::find(value.as_bytes(), literal).is_some(),
            };
        }
        let bytes = value.as_bytes();
        let (mut input, mut pattern) = (0, 0);
        let mut star = None;
        while input < bytes.len() {
            match self.tokens.get(pattern) {
                Some(LikeToken::Literal(byte)) if *byte == bytes[input] => {
                    input += 1;
                    pattern += 1;
                }
                Some(LikeToken::One) if !matches!(bytes[input], b'\r' | b'\n') => {
                    input += 1;
                    pattern += 1;
                }
                Some(LikeToken::Many) => {
                    star = Some((pattern, input));
                    pattern += 1;
                }
                _ => {
                    if let Some((position, consumed)) = star {
                        input = consumed + 1;
                        pattern = position + 1;
                        star = Some((position, input));
                    } else {
                        return false;
                    }
                }
            }
        }
        while self.tokens.get(pattern) == Some(&LikeToken::Many) {
            pattern += 1;
        }
        pattern == self.tokens.len()
    }
}

fn compatible_regex(pattern: &str) -> Option<String> {
    let mut escaped = false;
    let mut class = false;
    let mut previous = 0;
    let mut result = String::new();
    for byte in pattern.bytes() {
        let was_escaped = escaped;
        if escaped {
            if !b"dDsSwW\\.[](){}*+?|^$-".contains(&byte) {
                return None;
            }
            escaped = false;
        } else {
            match byte {
                b'\\' => escaped = true,
                b'[' if !class => class = true,
                b']' if class => class = false,
                b'[' | b']' | b'&' => return None,
                b'^' if class => {}
                b'?' | b'^' | b'$' | b'{' | b'}' => return None,
                b'*' | b'+' if previous == b')' || previous == b'*' || previous == b'+' => {
                    return None
                }
                _ => {}
            }
        }
        if byte == b'.' && !class && !was_escaped {
            result.push_str("[^\\r\\n]");
        } else {
            result.push(byte as char);
        }
        previous = byte;
    }
    (!escaped && !class).then_some(result)
}

fn sql_pattern(pattern: &str, escape: Option<&str>, similar: bool) -> Option<String> {
    // Flink validates special SIMILAR escapes before translation, including both
    // characters of a doubled escape. Keep those validation rules in its evaluator.
    if similar && escape.is_some_and(|s| s.bytes().any(|c| b"[]()|^-+*_%?{}:.$\\".contains(&c))) {
        return None;
    }
    let escape = match escape {
        Some(s) if s.len() == 1 && s.as_bytes()[0] != 0 => Some(s.as_bytes()[0]),
        Some(_) => return None,
        None => None,
    };
    let mut result = String::from("\\A(?:");
    if similar
        && (pattern.contains("(?")
            || pattern
                .as_bytes()
                .windows(2)
                .any(|pair| pair.iter().all(|byte| b"*+?".contains(byte))))
    {
        return None;
    }
    let mut bytes = pattern.bytes();
    while let Some(byte) = bytes.next() {
        if Some(byte) == escape {
            let next = bytes.next()?;
            if !b"_%".contains(&next) && Some(next) != escape {
                return None;
            }
            result.push_str(&regex::escape(&(next as char).to_string()));
        } else {
            match byte {
                b'%' => result.push_str("[\\s\\S]*"),
                b'_' => result.push_str("[^\\r\\n]"),
                b'.' if similar => result.push_str("[^\\r\\n]"),
                b'^' if similar => result.push('^'),
                b'|' | b'(' | b')' | b'*' | b'+' | b'?' if similar => result.push(byte as char),
                // SQL character classes and POSIX names require a separate verified translation.
                b'[' | b']' | b'{' | b'}' if similar => return None,
                _ => result.push_str(&regex::escape(&(byte as char).to_string())),
            }
        }
    }
    result.push_str(")\\z");
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn strings(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }
    #[test]
    fn java_empty_match_progression_and_literal_replacement() {
        let args = [
            strings(&["aa", "", "abc"]),
            strings(&["a*", "", ""]),
            strings(&["$1", "x", "X"]),
        ];
        let result = evaluate(2, &args, &DataType::Utf8).unwrap();
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec!["$1$1", "x", "XaXbXcX"])
        );
        let counts = evaluate(4, &args[..2], &DataType::Int32).unwrap();
        assert_eq!(
            counts.as_ref(),
            &arrow::array::Int32Array::from(vec![2, 1, 4])
        );
    }
    #[test]
    fn java_only_syntax_and_unicode_classes_never_guess() {
        for p in [
            "a(?=b)", "(a)\\1", "(?i)a", "[a&&b]", "a$", "(a(b)?)+", "\\p{L}",
        ] {
            assert!(compatible_regex(p).is_none(), "{p}");
        }
        let args = [strings(&["a1", "a\nb"]), strings(&["\\d", ".+"])];
        let result = evaluate(6, &args, &DataType::Utf8).unwrap();
        assert_eq!(result.as_ref(), &StringArray::from(vec!["1", "a"]));
    }
    #[test]
    fn escape_and_similar_use_whole_string_and_include_newlines() {
        for (op, s, p, e, expected) in [
            (7, "a_b", "a!_b", "!", true),
            (7, "a\nb", "a%b", "!", true),
            (8, "abc", "a(b|d)c", "!", true),
            (8, "xabc", "a(b|d)c", "!", false),
        ] {
            let result = evaluate(
                op,
                &[strings(&[s]), strings(&[p]), strings(&[e])],
                &DataType::Boolean,
            )
            .unwrap();
            assert_eq!(
                result.as_ref(),
                &arrow::array::BooleanArray::from(vec![expected])
            );
        }
    }

    #[test]
    fn like_specializations_agree_with_pattern_translation_for_empty_and_control_text() {
        fn words(alphabet: &[u8]) -> Vec<String> {
            let mut words = vec![String::new()];
            for _ in 0..4 {
                let previous = words.clone();
                for word in previous {
                    for byte in alphabet {
                        words.push(format!("{word}{}", *byte as char));
                    }
                }
            }
            words.sort();
            words.dedup();
            words
        }
        let values = words(b"ab\r\n");
        for pattern in words(b"ab%_") {
            let like = Like::compile(&pattern, None).unwrap();
            let regex = Regex::new(&sql_pattern(&pattern, None, false).unwrap()).unwrap();
            for value in &values {
                assert_eq!(
                    like.matches(value),
                    regex.is_match(value.as_bytes()),
                    "{value:?} {pattern:?}"
                );
            }
        }
    }

    #[test]
    fn similar_special_escape_validation_retains_the_java_evaluator() {
        for escape in [":", ".", "$", "\\", "%", "_", "+"] {
            assert!(sql_pattern("abc", Some(escape), true).is_none());
            assert!(sql_pattern(&escape.repeat(2), Some(escape), true).is_none());
        }
        assert!(sql_pattern("a!_b", Some("!"), true).is_some());
        for pattern in ["a++a", "a*+a", "a?+a", "a**", "a+?a"] {
            assert!(sql_pattern(pattern, None, true).is_none());
        }
    }
}
