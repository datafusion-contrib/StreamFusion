use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_binary_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_ascii_bytes",
        vec![DataType::Binary],
        DataType::Int32,
        ascii,
    )
}

fn ascii(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("ASCII expects one character argument");
    };
    let input = as_binary_array(input)?;
    // Flink widens a signed Java byte; it neither decodes nor validates a code point.
    let values = (0..input.len())
        .map(|row| {
            input
                .value(row)
                .first()
                .map_or(0, |byte| *byte as i8 as i32)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Int32Array::new(
        values.into(),
        input.nulls().cloned(),
    )))
}
