use super::{integer, text};
use arrow::array::{Array, ArrayRef, BooleanArray, Decimal128Array, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use std::fmt::Write;
use std::sync::Arc;

pub(super) fn evaluate(args: &[ArrayRef]) -> Option<ArrayRef> {
    let rows = args.first()?.len();
    if let Some(formats) = args[0].as_any().downcast_ref::<StringArray>() {
        if rows != 0 && formats.null_count() == 0 {
            let pattern = formats.value(0);
            if formats.iter().skip(1).all(|value| value == Some(pattern)) {
                return primitive_literal::<false>(pattern, &args[1..], rows);
            }
        }
    }
    let mut output = Vec::with_capacity(rows);
    for row in 0..args[0].len() {
        if args[0].is_null(row) {
            output.push(None);
            continue;
        }
        output.push(Some(format(text(&args[0], row)?, &args[1..], row)?));
    }
    Some(Arc::new(StringArray::from(output)))
}

fn format(pattern: &str, args: &[ArrayRef], row: usize) -> Option<String> {
    let mut output = String::with_capacity(pattern.len() + 32);
    let mut value = String::new();
    write_format::<false>(pattern, args, row, &mut output, &mut value)?;
    Some(output)
}

pub(super) fn primitive_literal<const BOOLEAN_OBJECTS: bool>(
    pattern: &str,
    args: &[ArrayRef],
    rows: usize,
) -> Option<ArrayRef> {
    let mut output = StringBuilder::with_capacity(rows, rows.saturating_mul(pattern.len() + 32));
    let mut scratch = String::with_capacity(pattern.len() + 32);
    let mut value = String::with_capacity(32);
    for row in 0..rows {
        scratch.clear();
        write_format::<BOOLEAN_OBJECTS>(pattern, args, row, &mut scratch, &mut value)?;
        output.append_value(&scratch);
    }
    Some(Arc::new(output.finish()))
}

fn write_format<const BOOLEAN_OBJECTS: bool>(
    pattern: &str,
    args: &[ArrayRef],
    row: usize,
    output: &mut String,
    value: &mut String,
) -> Option<()> {
    let mut digits = itoa::Buffer::new();
    let bytes = pattern.as_bytes();
    let mut i = 0;
    let mut ordinary = 0;
    let mut previous = None;
    while i < bytes.len() {
        if bytes[i] != b'%' {
            output.push(bytes[i] as char);
            i += 1;
            continue;
        }
        i += 1;
        if bytes.get(i) == Some(&b'%') {
            output.push('%');
            i += 1;
            continue;
        }
        // Platform newline and temporal/hash/float formatting retain the running JDK.
        let digits_start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        let explicit = if bytes.get(i) == Some(&b'$') {
            let index = pattern[digits_start..i]
                .parse::<usize>()
                .ok()?
                .checked_sub(1)?;
            i += 1;
            Some(index)
        } else {
            i = digits_start;
            None
        };
        let mut left = false;
        let mut zero = false;
        let mut plus = false;
        let mut space = false;
        let mut reuse = false;
        while let Some(flag) = bytes.get(i) {
            let seen = match flag {
                b'-' => &mut left,
                b'0' => &mut zero,
                b'+' => &mut plus,
                b' ' => &mut space,
                b'<' => &mut reuse,
                _ => break,
            };
            if *seen {
                return None;
            }
            *seen = true;
            i += 1;
        }
        if left && zero || plus && space || explicit.is_some() && reuse {
            return None;
        }
        let start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        let width = if start == i {
            0
        } else {
            pattern[start..i].parse::<usize>().ok()?
        };
        if width > 10000 || (left || zero) && width == 0 {
            return None;
        }
        let precision = if bytes.get(i) == Some(&b'.') {
            i += 1;
            let start = i;
            while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            Some(pattern[start..i].parse::<usize>().ok()?)
        } else {
            None
        };
        if precision.is_some_and(|p| p > i32::MAX as usize) {
            return None;
        }
        let conversion = *bytes.get(i)?;
        i += 1;
        if matches!(conversion, b's' | b'S' | b'b' | b'B') && (plus || space || zero) {
            return None;
        }
        if matches!(conversion, b'x' | b'X' | b'o') && (plus || space)
            || matches!(conversion, b'c' | b'C') && (precision.is_some() || plus || space || zero)
        {
            return None;
        }
        let index = if reuse {
            previous?
        } else if let Some(index) = explicit {
            index
        } else {
            let index = ordinary;
            ordinary += 1;
            index
        };
        previous = Some(index);
        let arg = args.get(index)?;
        value.clear();
        if arg.is_null(row) {
            if !matches!(
                conversion,
                b's' | b'S' | b'd' | b'x' | b'X' | b'o' | b'b' | b'B' | b'f' | b'c' | b'C'
            ) {
                return None;
            }
            value.push_str(if matches!(conversion, b'b' | b'B') {
                "false"
            } else {
                "null"
            });
        } else {
            match conversion {
                b's' | b'S' => {
                    if plus || space || zero {
                        return None;
                    }
                    if let Some(text) = text(arg, row) {
                        value.push_str(text);
                    } else if let Some(number) = integer(arg, row) {
                        value.push_str(digits.format(number));
                    } else if BOOLEAN_OBJECTS && arg.data_type() == &DataType::Boolean {
                        value.push_str(
                            if arg.as_any().downcast_ref::<BooleanArray>()?.value(row) {
                                "true"
                            } else {
                                "false"
                            },
                        );
                    } else if let Some(values) = arg.as_any().downcast_ref::<Decimal128Array>() {
                        *value = decimal_text(values.value(row), values.scale())?;
                    } else {
                        return None;
                    }
                }
                b'd' => {
                    if precision.is_some() {
                        return None;
                    }
                    let number = integer(arg, row)?;
                    if number >= 0 && plus {
                        value.push('+');
                    } else if number >= 0 && space {
                        value.push(' ');
                    }
                    value.push_str(digits.format(number));
                }
                // Formatter treats any non-NULL boxed integer, including zero, as true.
                b'b' | b'B'
                    if matches!(
                        arg.data_type(),
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                    ) =>
                {
                    value.push_str("true");
                }
                b'b' | b'B' if BOOLEAN_OBJECTS && arg.data_type() == &DataType::Boolean => {
                    value.push_str(if arg.as_any().downcast_ref::<BooleanArray>()?.value(row) {
                        "true"
                    } else {
                        "false"
                    });
                }
                b'f' => {
                    // The released SQL evaluator passes DecimalData, not BigDecimal.
                    // Formatter rejects it for %f; retain that evaluator for numeric formats.
                    return None;
                }
                b'c' | b'C' => {
                    if precision.is_some() || plus || space || zero {
                        return None;
                    }
                    if !matches!(
                        arg.data_type(),
                        DataType::Int8 | DataType::Int16 | DataType::Int32
                    ) {
                        return None;
                    }
                    let number = integer(arg, row)?;
                    if !(0..128).contains(&number) {
                        return None;
                    }
                    value.push(number as u8 as char);
                }
                b'x' | b'X' | b'o' => {
                    if precision.is_some() || plus || space {
                        return None;
                    }
                    let number = integer(arg, row)?;
                    let bits = match arg.data_type() {
                        DataType::Int8 => number as u8 as u64,
                        DataType::Int16 => number as u16 as u64,
                        DataType::Int32 => number as u32 as u64,
                        DataType::Int64 => number as u64,
                        _ => return None,
                    };
                    if conversion == b'o' {
                        write!(value, "{bits:o}").ok()?;
                    } else {
                        write!(value, "{bits:x}").ok()?;
                    }
                }
                _ => return None,
            }
        }
        if precision.is_some() && !matches!(conversion, b's' | b'S' | b'b' | b'B' | b'f') {
            return None;
        }
        if let Some(limit) = precision.filter(|_| conversion != b'f' || arg.is_null(row)) {
            value.truncate(value.len().min(limit));
        }
        if matches!(conversion, b'S' | b'X' | b'B' | b'C') {
            value.make_ascii_uppercase();
        }
        let padding = width.saturating_sub(value.len());
        if left {
            output.push_str(value);
            output.extend(std::iter::repeat_n(' ', padding));
        } else if zero && !arg.is_null(row) {
            if value.starts_with(['-', '+', ' ']) {
                output.push(value.remove(0));
            }
            output.extend(std::iter::repeat_n('0', padding));
            output.push_str(value);
        } else {
            output.extend(std::iter::repeat_n(' ', padding));
            output.push_str(value);
        }
    }
    Some(())
}

fn decimal_text(value: i128, scale: i8) -> Option<String> {
    if !(0..=38).contains(&scale) {
        return None;
    }
    let scale = scale as usize;
    let mut digits = value.unsigned_abs().to_string();
    if digits.len() <= scale {
        digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
    }
    if scale != 0 {
        digits.insert(digits.len() - scale, '.');
    }
    if value < 0 {
        digits.insert(0, '-');
    }
    Some(digits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int16Array, Int32Array, Int64Array, Int8Array};

    #[test]
    fn equal_and_varying_formats_preserve_decimal_nulls_and_batch_rejection() {
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12000), None, Some(-1), Some(0)])
                .with_precision_and_scale(20, 3)
                .unwrap(),
        );
        let original = values.to_data();
        for offset in 0..=values.len() {
            let rows = values.len() - offset;
            let formats: ArrayRef = Arc::new(StringArray::from(vec!["%s/%1$s"; rows]));
            let output = evaluate(&[formats, values.slice(offset, rows)]).unwrap();
            let expected = StringArray::from(vec![
                "12.000/12.000",
                "null/null",
                "-0.001/-0.001",
                "0.000/0.000",
            ]);
            assert_eq!(output.to_data(), expected.slice(offset, rows).to_data());
        }
        let formats: ArrayRef = Arc::new(StringArray::from(vec![
            Some("%s"),
            None,
            Some("%s/%1$s"),
            Some("%s"),
        ]));
        let output = evaluate(&[formats, values.clone()]).unwrap();
        assert_eq!(
            output.to_data(),
            StringArray::from(vec![
                Some("12.000"),
                None,
                Some("-0.001/-0.001"),
                Some("0.000")
            ])
            .to_data()
        );
        let invalid: ArrayRef = Arc::new(StringArray::from(vec!["%q"; values.len()]));
        assert!(evaluate(&[invalid, values.clone()]).is_none());
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true; values.len()]));
        let formats: ArrayRef = Arc::new(StringArray::from(vec!["%b"; values.len()]));
        assert!(evaluate(&[formats, booleans]).is_none());
        assert_eq!(values.to_data(), original);
    }

    #[test]
    fn boolean_integer_format_uses_object_nullness_not_numeric_truth() {
        for input in [
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                Some(0),
                Some(i8::MAX),
                None,
            ])) as ArrayRef,
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                Some(0),
                Some(i16::MAX),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                Some(0),
                Some(i32::MAX),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(0),
                Some(i64::MAX),
                None,
            ])),
        ] {
            let original = input.to_data();
            for offset in 0..=input.len() {
                let len = input.len() - offset;
                let output = primitive_literal::<false>(
                    "%b/%1$B/%1$.2b/%1$8.1B",
                    &[input.slice(offset, len)],
                    len,
                )
                .unwrap();
                let expected = StringArray::from(vec![
                    "true/TRUE/tr/       T",
                    "true/TRUE/tr/       T",
                    "true/TRUE/tr/       T",
                    "false/FALSE/fa/       F",
                ]);
                assert_eq!(output.to_data(), expected.slice(offset, len).to_data());
            }
            assert_eq!(input.to_data(), original);
            for pattern in ["%+b", "%0b", "%#B"] {
                assert!(format(pattern, &[input.clone()], 0).is_none());
            }
        }
    }

    #[test]
    fn java_indexing_sign_padding_twos_complement_and_nulls() {
        let args = [
            Arc::new(Int64Array::from(vec![-42])) as ArrayRef,
            Arc::new(Int32Array::from(vec![-1])) as ArrayRef,
        ];
        assert_eq!(
            format("%1$+08d|%2$x|%<X|%1$d|%%", &args, 0).as_deref(),
            Some("-0000042|ffffffff|FFFFFFFF|-42|%")
        );
        assert_eq!(format("%d %d", &args, 0).as_deref(), Some("-42 -1"));
        assert!(format("%q", &args, 0).is_none());
        assert!(format("%9$s", &args, 0).is_none());
        assert!(format("%.2f", &args, 0).is_none());
    }

    #[test]
    fn decimal_string_preserves_plain_notation_and_scale() {
        assert_eq!(decimal_text(12345, 3).as_deref(), Some("12.345"));
        assert_eq!(decimal_text(-1, 3).as_deref(), Some("-0.001"));
        assert_eq!(decimal_text(12345000, 6).as_deref(), Some("12.345000"));
        assert_eq!(decimal_text(10, 8).as_deref(), Some("0.00000010"));
        assert_eq!(decimal_text(0, 7).as_deref(), Some("0.0000000"));
        assert_eq!(decimal_text(1, 0).as_deref(), Some("1"));
        for scale in 1..=38 {
            let zeros = "0".repeat(scale as usize);
            let prefix = "0".repeat(scale as usize - 1);
            assert_eq!(decimal_text(0, scale), Some(format!("0.{zeros}")));
            assert_eq!(decimal_text(1, scale), Some(format!("0.{prefix}1")));
            assert_eq!(decimal_text(-1, scale), Some(format!("-0.{prefix}1")));
        }
        assert!(decimal_text(1, -1).is_none());
        assert!(decimal_text(1, 39).is_none());
    }

    #[test]
    fn high_scale_decimal_certificates_preserve_slices_formats_and_rejection() {
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(99), Some(1), Some(0), Some(-1), None])
                .with_precision_and_scale(19, 19)
                .unwrap()
                .slice(1, 4),
        );
        let text = [
            "0.0000000000000000001",
            "0.0000000000000000000",
            "-0.0000000000000000001",
            "null",
        ];
        let args = [values.clone()];
        for (row, expected) in text.iter().enumerate() {
            assert_eq!(format("%s", &args, row).as_deref(), Some(*expected));
            assert_eq!(
                format("%S", &args, row),
                Some(expected.to_ascii_uppercase())
            );
            assert_eq!(
                format("%1$24.4s", &args, row),
                Some(format!("{:>24}", &expected[..4]))
            );
            assert_eq!(format("%.0s", &args, row).as_deref(), Some(""));
        }
        for offset in 0..=4 {
            let formats: ArrayRef = Arc::new(StringArray::from(vec!["%s"; 4 - offset]));
            let output = evaluate(&[formats, values.slice(offset, 4 - offset)]).unwrap();
            assert_eq!(
                output.to_data(),
                StringArray::from(text[offset..].to_vec()).to_data()
            );
        }
        let formats: ArrayRef = Arc::new(StringArray::from(vec![
            Some("%.4s"),
            None,
            Some("%S"),
            Some("%s"),
        ]));
        assert_eq!(
            evaluate(&[formats, values.clone()]).unwrap().to_data(),
            StringArray::from(vec![Some("0.00"), None, Some(text[2]), Some("null")]).to_data()
        );
        for pattern in ["%f", "%d", "%x"] {
            let formats: ArrayRef = Arc::new(StringArray::from(vec![pattern; 4]));
            assert!(evaluate(&[formats, values.clone()]).is_none());
        }
    }

    #[test]
    fn uppercase_character_also_uppercases_the_null_literal() {
        let args = [Arc::new(Int32Array::from(vec![Some(97), None])) as ArrayRef];
        assert_eq!(format("%C", &args, 0).as_deref(), Some("A"));
        assert_eq!(format("%C", &args, 1).as_deref(), Some("NULL"));
        assert!(format("%.2147483648s", &args, 0).is_none());
        let longs = [Arc::new(Int64Array::from(vec![97])) as ArrayRef];
        assert!(format("%c", &longs, 0).is_none());
    }
}
