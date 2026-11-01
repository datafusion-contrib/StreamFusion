use std::sync::{Arc, OnceLock};

use arrow::array::{Array, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    static DIGITS: OnceLock<std::result::Result<Arc<[u64]>, String>> = OnceLock::new();
    let digits = DIGITS
        .get_or_init(|| super::string_classification::load_table("decimalDigits", 4096))
        .as_ref()
        .expect("worker JDK decimal digit table");
    ScalarUDF::new_from_impl(Decimal {
        digits: digits.clone(),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Decimal {
    digits: Arc<[u64]>,
    signature: Signature,
}

impl Decimal {
    fn test(&self, value: &str) -> bool {
        // ASCII floating syntax also accepts every ASCII integer, regardless of its magnitude.
        ascii_double(value.as_bytes()) || (!value.is_ascii() && self.unicode_long(value))
    }

    fn unicode_long(&self, value: &str) -> bool {
        let (value, limit) = match value.as_bytes().first() {
            Some(b'-') => (&value[1..], i64::MAX as u64 + 1),
            Some(b'+') => (&value[1..], i64::MAX as u64),
            _ => (value, i64::MAX as u64),
        };
        if value.is_empty() {
            return false;
        }
        let mut number = 0u64;
        for ch in value.chars() {
            let unit = ch as usize;
            if unit > 0xffff {
                return false;
            }
            let digit = (self.digits[unit >> 4] >> ((unit & 15) * 4)) & 15;
            if digit > 9 || number > (limit - digit) / 10 {
                return false;
            }
            number = number * 10 + digit;
        }
        true
    }
}

fn digits(bytes: &[u8], pos: &mut usize, hex: bool) -> bool {
    let start = *pos;
    while *pos < bytes.len()
        && if hex {
            bytes[*pos].is_ascii_hexdigit()
        } else {
            bytes[*pos].is_ascii_digit()
        }
    {
        *pos += 1;
    }
    *pos != start
}

fn ascii_double(mut bytes: &[u8]) -> bool {
    while bytes.first().is_some_and(|b| *b <= b' ') {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(|b| *b <= b' ') {
        bytes = &bytes[..bytes.len() - 1];
    }
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        bytes = &bytes[1..];
    }
    if matches!(bytes, b"NaN" | b"Infinity") {
        return true;
    }
    let hex = bytes.len() >= 2 && bytes[0] == b'0' && matches!(bytes[1], b'x' | b'X');
    let mut pos = if hex { 2 } else { 0 };
    let whole = digits(bytes, &mut pos, hex);
    let fraction = if bytes.get(pos) == Some(&b'.') {
        pos += 1;
        digits(bytes, &mut pos, hex)
    } else {
        false
    };
    if !whole && !fraction {
        return false;
    }
    let exponent = if hex { b'p' } else { b'e' };
    if bytes
        .get(pos)
        .is_some_and(|b| b.to_ascii_lowercase() == exponent)
    {
        pos += 1;
        if matches!(bytes.get(pos), Some(b'+' | b'-')) {
            pos += 1;
        }
        if !digits(bytes, &mut pos, false) {
            return false;
        }
    } else if hex {
        return false;
    }
    if matches!(bytes.get(pos), Some(b'f' | b'F' | b'd' | b'D')) {
        pos += 1;
    }
    pos == bytes.len()
}

impl ScalarUDFImpl for Decimal {
    fn name(&self) -> &str {
        "flink_is_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Utf8] => Ok(DataType::Boolean),
            _ => exec_err!("IS_DECIMAL requires one character argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.as_slice() {
            [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(value.as_ref().is_some_and(|value| self.test(value)))),
            )),
            [ColumnarValue::Array(input)] => {
                let input = datafusion::common::cast::as_string_array(input)?;
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
                    BooleanBuffer::collect_bool(input.len(), |row| {
                        input.is_valid(row) && self.test(input.value(row))
                    }),
                    None,
                ))))
            }
            _ => exec_err!("IS_DECIMAL requires one character argument"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn function() -> Decimal {
        let mut digits = vec![u64::MAX; 4096];
        for zero in [0x30, 0x660, 0x966, 0xff10] {
            for digit in 0..10 {
                let unit = zero + digit;
                let shift = (unit & 15) * 4;
                digits[unit >> 4] =
                    (digits[unit >> 4] & !(15 << shift)) | ((digit as u64) << shift);
            }
        }
        Decimal {
            digits: digits.into(),
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }

    #[test]
    fn java_double_lexical_grammar_accepts_extreme_magnitudes_and_rejects_nonjava_forms() {
        for value in [
            "0",
            "-0",
            "+1",
            "1.",
            ".5",
            "-1.e3",
            "1f",
            "1D",
            "1e-99999999999",
            "1e99999999999",
            "NaN",
            "+NaN",
            "-Infinity",
            "\0\t 1 \n\0",
            "0x1p0",
            "-0X1.Fp+99F",
            "0x.8p-1",
            "0x1.p0",
            "0x1p99999999999",
        ] {
            assert!(ascii_double(value.as_bytes()), "{value:?}");
        }
        for value in [
            "",
            " ",
            "+",
            ".",
            "1e",
            "1e+",
            "1e1.0",
            "1_0",
            "nan",
            "inf",
            "NaNf",
            "InfinityD",
            "0x1",
            "0xp1",
            "0x.p1",
            "0x1p",
            "0x1p1.0",
            "0x1p0L",
            "\u{a0}1",
            "1\u{a0}",
            "\u{661}.0",
            "1\0x",
            "1\n2",
            "\u{ff11}",
        ] {
            assert!(!ascii_double(value.as_bytes()), "{value:?}");
        }
    }

    #[test]
    fn unicode_integer_path_uses_utf16_digit_values_and_signed_long_bounds_without_trimming() {
        let function = function();
        for value in [
            "\u{661}",
            "1\u{ff11}",
            "-\u{967}",
            "+\u{661}",
            "922337203685477580\u{667}",
            "-922337203685477580\u{668}",
            "000000000000000000000000\u{661}",
        ] {
            assert!(function.test(value), "{value:?}");
        }
        for value in [
            "922337203685477580\u{668}",
            "-922337203685477580\u{669}",
            " \u{661}",
            "\u{661} ",
            "\u{661}.0",
            "\u{661}e1",
            "\u{661}f",
            "\u{b2}",
            "\u{1d7ce}",
            "?",
            "\u{fffd}",
            "\u{6f1}",
        ] {
            assert!(!function.test(value), "{value:?}");
        }
        assert!(
            function.test("9223372036854775808"),
            "ASCII takes the double path"
        );
    }

    #[test]
    fn slices_nulls_and_empty_batches_produce_owned_nonnull_predicates() {
        use arrow::array::StringArray;
        use arrow::datatypes::Field;

        let function = function();
        let input = Arc::new(StringArray::from(vec![
            Some("unused"),
            Some("NaN"),
            None,
            Some(""),
        ])) as arrow::array::ArrayRef;
        for len in [0, 3] {
            let result = function
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(input.slice(1, len))],
                    arg_fields: vec![Arc::new(Field::new("v", DataType::Utf8, true))],
                    number_rows: len,
                    return_field: Arc::new(Field::new("v", DataType::Boolean, false)),
                    config_options: Arc::new(Default::default()),
                })
                .unwrap();
            let ColumnarValue::Array(result) = result else {
                panic!("expected column")
            };
            assert_eq!(
                result.as_any().downcast_ref::<BooleanArray>().unwrap(),
                &BooleanArray::from(vec![true, false, false]).slice(0, len)
            );
            assert_eq!(result.null_count(), 0);
            result.to_data().validate_full().unwrap();
        }
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
                arg_fields: vec![Arc::new(Field::new("v", DataType::Utf8, true))],
                number_rows: 0,
                return_field: Arc::new(Field::new("v", DataType::Boolean, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))
        ));
    }
}
