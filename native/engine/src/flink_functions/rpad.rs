use arrow::datatypes::DataType;
use datafusion::logical_expr::ScalarUDF;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_rpad",
        vec![DataType::Utf8, DataType::Int32, DataType::Utf8],
        DataType::Utf8,
        |args| super::lpad::pad(args, false),
    )
}
