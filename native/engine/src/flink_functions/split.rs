use arrow::array::{Array, ArrayRef, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_split",
        vec![DataType::Utf8; 2],
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        split,
    )
}

fn split(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input, separator] = args else {
        return exec_err!("SPLIT expects two arguments");
    };
    let input = as_string_array(input)?;
    let separator = as_string_array(separator)?;
    let mut output = ListBuilder::with_capacity(
        StringBuilder::with_capacity(input.len(), input.values().len()),
        input.len(),
    );
    for row in 0..input.len() {
        if input.is_null(row) || separator.is_null(row) {
            output.append(false);
            continue;
        }
        let separator = separator.value(row);
        if separator.is_empty() {
            return exec_err!("SPLIT requires a non-empty separator");
        }
        if !input.value(row).is_empty() {
            if separator.len() == 1 {
                // The char searcher uses memchr for ASCII instead of a substring search.
                for piece in input.value(row).split(separator.as_bytes()[0] as char) {
                    output.values().append_value(piece);
                }
            } else {
                for piece in input.value(row).split(separator) {
                    output.values().append_value(piece);
                }
            }
        }
        output.append(true);
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListArray, StringArray};

    #[test]
    fn split_keeps_empty_tokens_and_an_empty_input_is_an_empty_array() {
        let output = split(&[
            Arc::new(StringArray::from(vec![Some("|a||"), Some(""), None])),
            Arc::new(StringArray::from(vec!["|"; 3])),
        ])
        .unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            as_string_array(&output.value(0)).unwrap(),
            &StringArray::from(vec!["", "a", "", ""])
        );
        assert_eq!(output.value_length(1), 0);
        assert!(output.is_null(2));
        assert!(split(&[]).is_err());
    }
}
