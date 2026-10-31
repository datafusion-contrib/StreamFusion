use std::sync::{Arc, OnceLock};

use arrow::array::{Array, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn alpha() -> ScalarUDF {
    static LETTERS: OnceLock<std::result::Result<Arc<[u64]>, String>> = OnceLock::new();
    let letters = LETTERS
        .get_or_init(load_letters)
        .as_ref()
        .expect("worker JDK character table");
    ScalarUDF::new_from_impl(Classification {
        letters: letters.clone(),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
}

fn load_letters() -> std::result::Result<Arc<[u64]>, String> {
    let vm = streamfusion_bridge::JVM
        .get()
        .ok_or_else(|| "JVM not captured for character-table initialization".to_owned())?;
    let mut env = vm.attach_current_thread().map_err(|e| e.to_string())?;
    let table: jni::errors::Result<Vec<i64>> = env.with_local_frame(4, |env| {
        let array = env
            .call_static_method(
                "tech/streamfusion/operator/JdkCharacterTable",
                "letters",
                "()[J",
                &[],
            )?
            .l()?;
        let array = jni::objects::JLongArray::from(array);
        let mut table = vec![0; env.get_array_length(&array)? as usize];
        env.get_long_array_region(&array, 0, &mut table)?;
        Ok(table)
    });
    let table = table.map_err(|e| e.to_string())?;
    if table.len() != 1024 {
        return Err("Worker JDK letter table has an invalid length".into());
    }
    Ok(table.into_iter().map(|word| word as u64).collect())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Classification {
    letters: Arc<[u64]>,
    signature: Signature,
}

impl Classification {
    fn test(&self, value: &str) -> bool {
        if value.is_empty() {
            return false;
        }
        if value.bytes().all(|byte| byte.is_ascii_alphabetic()) {
            return true;
        }
        // Commons Lang classifies UTF-16 chars, so even supplementary letters fail.
        value.chars().all(|ch| {
            let unit = ch as usize;
            unit <= 0xffff && self.letters[unit >> 6] & (1 << (unit & 63)) != 0
        })
    }
}

impl ScalarUDFImpl for Classification {
    fn name(&self) -> &str {
        "flink_is_alpha"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Utf8] => Ok(DataType::Boolean),
            _ => exec_err!("IS_ALPHA requires one character argument"),
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
            _ => exec_err!("IS_ALPHA requires one character argument"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;

    fn classifier() -> Classification {
        let mut bits = vec![0; 1024];
        for unit in ('a'..='z').chain('A'..='Z').chain(['\u{4e2d}', '\u{e9}']) {
            let unit = unit as usize;
            bits[unit >> 6] |= 1 << (unit & 63);
        }
        Classification {
            letters: bits.into(),
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }

    #[test]
    fn ascii_unicode_supplementary_and_replacement_units_use_the_supplied_table() {
        let function = classifier();
        for value in ["a", "AZaz", "\u{4e2d}", "\u{e9}a"] {
            assert!(function.test(value), "{value:?}");
        }
        for value in [
            "",
            "abc1",
            "a b",
            "a\0",
            "\u{301}",
            "\u{10400}",
            "?",
            "\u{fffd}",
        ] {
            assert!(!function.test(value), "{value:?}");
        }
        assert!(
            !function.test("\u{3a9}"),
            "uses the supplied JDK table, not Rust's Unicode tables"
        );
    }

    #[test]
    fn sliced_nullable_and_empty_arrays_return_nonnull_booleans_without_mutating_input() {
        let function = classifier();
        let input = Arc::new(StringArray::from(vec![
            Some("unused"),
            Some("Ab"),
            None,
            Some(""),
        ])) as arrow::array::ArrayRef;
        for len in [0, 3] {
            let input = input.slice(1, len);
            let result = function
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(input.clone())],
                    arg_fields: vec![Arc::new(Field::new("v", DataType::Utf8, true))],
                    number_rows: len,
                    return_field: Arc::new(Field::new("result", DataType::Boolean, false)),
                    config_options: Arc::new(Default::default()),
                })
                .unwrap();
            let ColumnarValue::Array(result) = result else {
                panic!("expected a column")
            };
            let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(
                result,
                &BooleanArray::from(vec![true, false, false]).slice(0, len)
            );
            assert_eq!(result.null_count(), 0);
            assert_eq!(input.null_count(), usize::from(len > 0));
            result.to_data().validate_full().unwrap();
        }
    }
}
