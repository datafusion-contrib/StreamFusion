use super::{integer, text};
use arrow::array::{ArrayRef, Decimal128Array, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use std::fmt::Write;
use std::sync::Arc;

pub(super) fn evaluate(args: &[ArrayRef]) -> Option<ArrayRef> {
    let mut output = Vec::with_capacity(args.first()?.len());
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
    write_format(pattern, args, row, &mut output)?;
    Some(output)
}

pub(super) fn integer_literal(pattern: &str, args: &[ArrayRef], rows: usize) -> Option<ArrayRef> {
    let mut output = StringBuilder::with_capacity(rows, rows.saturating_mul(pattern.len() + 32));
    let mut scratch = String::with_capacity(pattern.len() + 32);
    for row in 0..rows {
        scratch.clear();
        write_format(pattern, args, row, &mut scratch)?;
        output.append_value(&scratch);
    }
    Some(Arc::new(output.finish()))
}

fn write_format(pattern: &str, args: &[ArrayRef], row: usize, output: &mut String) -> Option<()> {
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
        let mut value = if arg.is_null(row) {
            if !matches!(
                conversion,
                b's' | b'S' | b'd' | b'x' | b'X' | b'o' | b'b' | b'B' | b'f' | b'c' | b'C'
            ) {
                return None;
            }
            if matches!(conversion, b'b' | b'B') {
                "false".to_owned()
            } else {
                "null".to_owned()
            }
        } else {
            match conversion {
                b's' | b'S' => {
                    if plus || space || zero {
                        return None;
                    }
                    if let Some(text) = text(arg, row) {
                        text.to_owned()
                    } else if let Some(value) = integer(arg, row) {
                        value.to_string()
                    } else if let Some(values) = arg.as_any().downcast_ref::<Decimal128Array>() {
                        decimal_text(values.value(row), values.scale())?
                    } else {
                        return None;
                    }
                }
                b'd' => {
                    if precision.is_some() {
                        return None;
                    }
                    let value = integer(arg, row)?;
                    if value >= 0 && plus {
                        format!("+{value}")
                    } else if value >= 0 && space {
                        format!(" {value}")
                    } else {
                        value.to_string()
                    }
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
                    let value = integer(arg, row)?;
                    if !(0..128).contains(&value) {
                        return None;
                    }
                    (value as u8 as char).to_string()
                }
                b'x' | b'X' | b'o' => {
                    if precision.is_some() || plus || space {
                        return None;
                    }
                    let value = integer(arg, row)?;
                    let bits = match arg.data_type() {
                        DataType::Int8 => value as u8 as u64,
                        DataType::Int16 => value as u16 as u64,
                        DataType::Int32 => value as u32 as u64,
                        DataType::Int64 => value as u64,
                        _ => return None,
                    };
                    let mut result = String::new();
                    if conversion == b'o' {
                        write!(result, "{bits:o}").ok()?;
                    } else {
                        write!(result, "{bits:x}").ok()?;
                    }
                    result
                }
                _ => return None,
            }
        };
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
            output.push_str(&value);
            output.extend(std::iter::repeat_n(' ', padding));
        } else if zero && !arg.is_null(row) {
            if value.starts_with(['-', '+', ' ']) {
                output.push(value.remove(0));
            }
            output.extend(std::iter::repeat_n('0', padding));
            output.push_str(&value);
        } else {
            output.extend(std::iter::repeat_n(' ', padding));
            output.push_str(&value);
        }
    }
    Some(())
}

fn fixed_decimal(value: i128, scale: i8) -> Option<String> {
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

fn decimal_text(value: i128, scale: i8) -> Option<String> {
    if !(0..=38).contains(&scale) {
        return None;
    }
    let mut digits = value.unsigned_abs().to_string();
    let exponent = digits.len() as i32 - i32::from(scale) - 1;
    if exponent >= -6 {
        return fixed_decimal(value, scale);
    }
    if digits.len() > 1 {
        digits.insert(1, '.');
    }
    if value < 0 {
        digits.insert(0, '-');
    }
    Some(format!("{digits}E{exponent}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
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
    fn decimal_string_preserves_scale_and_scientific_notation() {
        assert_eq!(decimal_text(12345, 3).as_deref(), Some("12.345"));
        assert_eq!(decimal_text(-1, 3).as_deref(), Some("-0.001"));
        assert_eq!(decimal_text(12345000, 6).as_deref(), Some("12.345000"));
        assert_eq!(decimal_text(10, 8).as_deref(), Some("1.0E-7"));
        assert_eq!(decimal_text(0, 7).as_deref(), Some("0E-7"));
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
