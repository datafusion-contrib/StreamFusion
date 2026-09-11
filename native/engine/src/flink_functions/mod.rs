//! Flink scalar registrations and kernels for semantics that differ from DataFusion.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{
    DataType, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use std::sync::Arc;

pub(crate) mod calendar;
pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod json_quote;
pub(crate) mod json_unquote;
pub(crate) mod left;
pub(crate) mod lpad;
pub(crate) mod right;
pub(crate) mod rpad;
pub(crate) mod split;
pub(crate) mod split_index;
pub(crate) mod substring;
pub(crate) mod to_date;

mod binary_strings;
mod charset;
mod is_json;
mod json_exists;
mod json_object;
mod json_path;
mod json_serialize;
mod json_value;
mod locate;
mod scalar;

const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

pub(crate) fn function(op: i64, arity: usize) -> Option<ScalarUDF> {
    Some(match op {
        100 => datafusion::functions::string::starts_with()
            .as_ref()
            .clone(),
        101 => datafusion::functions::string::ends_with().as_ref().clone(),
        102 => datafusion::functions::unicode::strpos().as_ref().clone(),
        103 => ScalarUDF::new_from_impl(locate::FlinkLocate::new()),
        104 => udf(
            "flink_bin",
            vec![DataType::Int64],
            DataType::Utf8,
            binary_strings::bin,
        ),
        105 => udf(
            "flink_hex_int",
            vec![DataType::Int64],
            DataType::Utf8,
            binary_strings::hex_int,
        ),
        106 => udf(
            "flink_hex_string",
            vec![DataType::Utf8],
            DataType::Utf8,
            |args| binary_strings::encode(args, false),
        ),
        107 => udf(
            "flink_to_base64",
            vec![DataType::Utf8],
            DataType::Utf8,
            |args| binary_strings::encode(args, true),
        ),
        108 => udf(
            "flink_unhex",
            vec![DataType::Utf8],
            DataType::Binary,
            binary_strings::unhex,
        ),
        109 => scalar::extremum(true),
        110 => scalar::extremum(false),
        111 => udf(
            "flink_initcap",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::initcap,
        ),
        112 => udf(
            "flink_translate",
            vec![DataType::Utf8; 3],
            DataType::Utf8,
            scalar::translate,
        ),
        113 => datafusion::functions::string::btrim().as_ref().clone(),
        114 => scalar::elt_function(arity),
        115 => udf(
            "flink_url_encode",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_encode,
        ),
        116 => {
            let mut types = vec![DataType::Int64; arity];
            for datatype in types.iter_mut().take(2) {
                *datatype = DataType::Utf8;
            }
            udf("flink_overlay", types, DataType::Utf8, scalar::overlay)
        }
        117 => udf(
            "flink_url_decode",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_decode,
        ),
        118 => udf(
            "flink_url_decode_ascii",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_decode_ascii,
        ),
        120 => encode::function(),
        121 => decode::function(),
        122 => json_quote::function(),
        123 => json_unquote::function(),
        124 => split::function(),
        125 => substring::function(arity),
        126 => left::function(),
        127 => right::function(),
        128 => lpad::function(),
        129 => rpad::function(),
        130 => split_index::function(),
        131 => to_date::function(),
        133 => calendar::function(calendar::Field::Quarter),
        134 => calendar::function(calendar::Field::Week),
        135 => calendar::function(calendar::Field::DayOfYear),
        136 => calendar::function(calendar::Field::DayOfWeek),
        139 => datafusion::functions::string::ltrim().as_ref().clone(),
        140 => datafusion::functions::string::rtrim().as_ref().clone(),
        141 => json_value::function(),
        142 => json_exists::function(),
        144 => is_json::function(is_json::JsonType::Value),
        145 => is_json::function(is_json::JsonType::Object),
        146 => is_json::function(is_json::JsonType::Array),
        147 => is_json::function(is_json::JsonType::Scalar),
        148 => json_value::typed_function(json_value::ReturnType::Boolean),
        149 => json_value::typed_function(json_value::ReturnType::Integer),
        150 => json_value::typed_function(json_value::ReturnType::Double),
        151 => udf(
            "flink_to_base64_binary",
            vec![DataType::Binary],
            DataType::Utf8,
            |args| binary_strings::encode(args, true),
        ),
        152 => json_serialize::function(),
        153 => json_object::function(),
        _ => return None,
    })
}

fn udf(
    name: &str,
    inputs: Vec<DataType>,
    output: DataType,
    kernel: fn(&[ArrayRef]) -> Result<ArrayRef>,
) -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        name,
        inputs,
        output,
        Volatility::Immutable,
        Arc::new(datafusion::functions::utils::make_scalar_function(
            kernel,
            vec![],
        )),
    )
}

fn map_timestamp_millis(input: &ArrayRef, map: impl Fn(i64) -> i64) -> Result<Int64Array> {
    match input.data_type() {
        DataType::Timestamp(TimeUnit::Second, None) => {
            Ok(as_primitive_array::<TimestampSecondType>(input)?
                .unary::<_, Int64Type>(|value| map(value.wrapping_mul(1000))))
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Ok(as_primitive_array::<TimestampMillisecondType>(input)?.unary::<_, Int64Type>(map))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(as_primitive_array::<TimestampMicrosecondType>(input)?
                .unary::<_, Int64Type>(|value| map(value.div_euclid(1000))))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Ok(as_primitive_array::<TimestampNanosecondType>(input)?
                .unary::<_, Int64Type>(|value| map(value.div_euclid(1_000_000))))
        }
        other => exec_err!("Expected plain TIMESTAMP, got {other}"),
    }
}

fn check_string_capacity(current: usize, additional: usize) -> Result<()> {
    if current
        .checked_add(additional)
        .is_none_or(|size| size > i32::MAX as usize)
    {
        return exec_err!("Function output exceeds Arrow string capacity");
    }
    Ok(())
}
