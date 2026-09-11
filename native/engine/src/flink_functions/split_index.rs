use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{
    cast::{as_int32_array, as_string_array},
    exec_err, Result,
};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_split_index",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int32],
        DataType::Utf8,
        split_index,
    )
}

fn split_index(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input, separator, index] = args else {
        return exec_err!("SPLIT_INDEX expects three arguments");
    };
    let input = as_string_array(input)?;
    let separator = as_string_array(separator)?;
    let index = as_int32_array(index)?;
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    for row in 0..input.len() {
        if input.is_null(row)
            || separator.is_null(row)
            || index.is_null(row)
            || index.value(row) < 0
            || input.value(row).is_empty()
        {
            output.append_null();
            continue;
        }
        let token = if separator.value(row).is_empty() {
            input
                .value(row)
                .split(java_whitespace)
                .nth(index.value(row) as usize)
        } else {
            input
                .value(row)
                .split(separator.value(row))
                .nth(index.value(row) as usize)
        };
        match token {
            Some(value) => output.append_value(value),
            None => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

fn java_whitespace(ch: char) -> bool {
    matches!(ch, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}'
        | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}'
        | '\u{205f}' | '\u{3000}')
}
