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
        "flink_right",
        vec![DataType::Utf8, DataType::Int32],
        DataType::Utf8,
        right,
    )
}

fn right(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input, count] = args else {
        return exec_err!("RIGHT expects two arguments");
    };
    let input = as_string_array(input)?;
    let count = as_int32_array(count)?;
    let mut output = StringBuilder::with_capacity(input.len(), input.values().len());
    for row in 0..input.len() {
        if input.is_null(row) || count.is_null(row) {
            output.append_null();
            continue;
        }
        let value = input.value(row);
        let start =
            super::substring::reverse_offset(value, count.value(row).max(0) as usize).unwrap_or(0);
        output.append_value(&value[start..]);
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    #[test]
    fn codepoints_counts_nulls_and_slices() {
        let input = StringArray::from(vec![
            Some("skip"),
            Some("a\u{1f600}b"),
            Some("abc"),
            Some("abc"),
            None,
        ])
        .slice(1, 4);
        let count = Int32Array::from(vec![2, i32::MIN, i32::MAX, 1]);
        let output = right(&[Arc::new(input), Arc::new(count)]).unwrap();
        assert_eq!(
            as_string_array(&output).unwrap(),
            &StringArray::from(vec![Some("\u{1f600}b"), Some(""), Some("abc"), None])
        );
        assert!(right(&[]).is_err());
    }
}
