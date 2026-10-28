//! Flink scalar registrations and kernels for semantics that differ from DataFusion.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{
    DataType, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use std::sync::Arc;

pub(crate) mod binary_cast;
pub(crate) mod binary_elt;
pub(crate) mod calendar;
mod calendar_round;
pub(crate) mod certified;
pub(crate) mod clock;
pub(crate) mod decimal;
pub(crate) mod decimal_float;
pub(crate) mod decimal_round_text;
pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod from_unixtime;
pub(crate) mod json_quote;
pub(crate) mod json_unquote;
pub(crate) mod left;
pub(crate) mod lpad;
pub(crate) mod random;
pub(crate) mod right;
pub(crate) mod rpad;
pub(crate) mod split;
pub(crate) mod split_index;
pub(crate) mod substring;
pub(crate) mod to_date;

mod abs;
pub(crate) mod array_cast;
mod array_concat;
mod array_constructor;
mod array_copy;
mod array_distinct;
mod array_edit;
#[cfg(test)]
mod array_extrema_tests;
pub(crate) mod array_item;
mod array_join;
mod array_remove;
mod array_search;
mod array_set;
mod array_slice;
mod array_sort;
mod array_timsort;
mod ascii;
mod binary_strings;
mod cardinality;
mod charset;
mod chr;
pub(crate) mod collection_cast;
mod collection_string;
mod element;
mod from_base64;
mod hash_code;
mod if_null;
mod instr;
pub(crate) mod integer_divide;
pub(crate) mod integer_string;
mod is_json;
mod java_timsort;
mod json_exists;
mod json_object;
mod json_path;
mod json_serialize;
mod json_value;
mod locate;
mod map_arrays;
mod map_constructor;
pub(crate) mod map_lookup;
mod map_projection;
mod map_union;
mod negate;
pub(crate) mod numeric;
pub(crate) mod numeric_cast;
mod row_constructor;
mod scalar;
pub(crate) mod sha2_runtime;
mod sign;
pub(crate) mod string_fusion;
mod string_to_boolean;
mod temporal_arithmetic;
mod temporal_extremum;
mod temporal_round;
mod timestamp_millis;
mod type_of;
mod unnest_temporal;

const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

pub(crate) fn function(op: i64, arity: usize) -> Option<ScalarUDF> {
    Some(match op {
        65 => sign::function(),
        67 => ascii::function(),
        81 => chr::function(),
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
        151 => binary_strings::base64_binary(),
        152 => json_serialize::function(),
        153 => json_object::function(),
        154 => temporal_round::function(),
        155 => timestamp_millis::function(timestamp_millis::Operation::Millis),
        156 => timestamp_millis::function(timestamp_millis::Operation::SubtractMillis),
        157 => timestamp_millis::function(timestamp_millis::Operation::SubtractMonths),
        158 => if_null::function(),
        159 => string_to_boolean::function(false),
        160 => string_to_boolean::function(true),
        161 => instr::function(arity),
        162 => hash_code::function(),
        165 => array_distinct::function(),
        166 => datafusion::functions::string::uuid().as_ref().clone(),
        167 => cardinality::function(),
        168 => type_of::function(),
        169 => array_copy::reverse(),
        170 => array_slice::function(arity),
        171 => array_constructor::function(arity, false),
        172 => array_constructor::function(arity, true),
        173 => unnest_temporal::function(unnest_temporal::Operation::EpochMillis),
        174 => unnest_temporal::function(unnest_temporal::Operation::UtcDate),
        175 => unnest_temporal::function(unnest_temporal::Operation::UtcTime),
        176 => unnest_temporal::function(unnest_temporal::Operation::ZonedDate),
        177 => unnest_temporal::function(unnest_temporal::Operation::ZonedTime),
        178 => array_search::function(true),
        179 => array_search::function(false),
        180 => datafusion::functions_nested::min_max::array_min_udf()
            .as_ref()
            .clone(),
        181 => datafusion::functions_nested::min_max::array_max_udf()
            .as_ref()
            .clone(),
        182 => array_edit::function(false),
        183 => array_edit::function(true),
        184 => array_remove::function(),
        185 => array_concat::function(arity),
        186 => datafusion::functions_nested::set_ops::array_union_udf()
            .as_ref()
            .clone(),
        187 => array_set::function(false),
        188 => array_set::function(true),
        189 => datafusion::functions_nested::sort::array_sort_udf()
            .as_ref()
            .clone(),
        190 => element::function(false),
        191 => map_arrays::function(),
        192 => map_projection::function(0),
        193 => map_projection::function(1),
        194 => map_projection::function(2),
        195 => map_union::function(arity),
        196 => map_constructor::function(arity),
        197 => datafusion::functions::core::named_struct().as_ref().clone(),
        198 => datafusion::functions::core::named_struct().as_ref().clone(),
        199 => datafusion::functions::core::named_struct().as_ref().clone(),
        200 => array_join::function(arity),
        201 => element::function(true),
        202 => json_value::java_input_function(),
        203 => calendar::function(calendar::Field::Year),
        204 => calendar::function(calendar::Field::Month),
        205 => calendar::function(calendar::Field::Day),
        206 => calendar::function(calendar::Field::Hour),
        207 => calendar::function(calendar::Field::Minute),
        208 => calendar::function(calendar::Field::Second),
        209 => calendar_round::function(),
        210 => temporal_arithmetic::function(temporal_arithmetic::Operation::TimestampMillis),
        211 => temporal_arithmetic::function(temporal_arithmetic::Operation::DateMillis),
        212 => temporal_arithmetic::function(temporal_arithmetic::Operation::TimeMillis),
        213 => temporal_arithmetic::function(temporal_arithmetic::Operation::DateMonths),
        214 => temporal_arithmetic::function(temporal_arithmetic::Operation::TimestampMonths),
        215 => temporal_extremum::function(arity, true),
        216 => temporal_extremum::function(arity, false),
        217 => array_sort::function(arity),
        218 => abs::function(),
        219 => negate::function(),
        220 => array_set::union(),
        221 => row_constructor::function(true),
        222 => row_constructor::function(false),
        223 => collection_string::split(),
        224 => array_timsort::function(arity),
        225 => map_constructor::binary_function(arity),
        226 => collection_string::literal(),
        227 => collection_string::map_input(),
        228 => collection_string::extremum(true),
        229 => collection_string::extremum(false),
        232 => from_base64::function(),
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

pub(crate) fn map_timestamp_millis(
    input: &ArrayRef,
    map: impl Fn(i64) -> i64,
) -> Result<Int64Array> {
    match input.data_type() {
        data_type if streamfusion_bridge::timestamp::is_component_timestamp(data_type) => Ok(
            streamfusion_bridge::timestamp::TimestampColumn::try_new(input.as_ref())?
                .to_millis()?
                .unary(map),
        ),
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
