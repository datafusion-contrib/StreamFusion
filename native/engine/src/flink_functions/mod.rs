//! Flink scalar registrations and kernels for semantics that differ from DataFusion.

use crate::expression_protocol::scalar_op;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{
    DataType, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use std::sync::Arc;

pub(crate) mod binary_cast;
mod binary_character;
pub(crate) mod binary_elt;
pub(crate) mod calendar;
mod calendar_round;
pub(crate) mod certified;
pub(crate) mod clock;
pub(crate) mod decimal;
mod decimal_epoch;
pub(crate) mod decimal_float;
pub(crate) mod decimal_round_text;
pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod from_unixtime;
mod integral_round;
pub(crate) mod json_quote;
pub(crate) mod json_unquote;
pub(crate) mod left;
pub(crate) mod lpad;
pub(crate) mod random;
pub(crate) mod right;
pub(crate) mod rpad;
pub(crate) mod split;
pub(crate) mod split_index;
pub(crate) mod string_comparison;
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
mod binary_boundary;
mod binary_strings;
mod boolean_string;
pub(crate) mod byte_ascii;
pub(crate) mod byte_concat;
pub(crate) mod byte_concat_ws;
pub(crate) mod byte_string_text;
mod cardinality;
mod charset;
mod chr;
pub(crate) mod collection_cast;
mod collection_string;
pub(crate) mod date_format;
pub(crate) mod decimal_string;
mod element;
mod from_base64;
mod hash_code;
mod if_null;
mod instr;
pub(crate) mod integer_divide;
mod integer_printf;
pub(crate) mod integer_string;
mod is_json;
mod java_timsort;
mod json_array;
mod json_container;
mod json_exists;
mod json_object;
mod json_path;
pub(crate) use json_path::with_projection_readers;
mod json_query;
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
mod numeric_classification;
mod round_position;
mod row_constructor;
mod scalar;
pub(crate) mod sha2_runtime;
mod sign;
mod string_classification;
mod string_decimal;
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
        scalar_op::STRING_IS_ALPHA => string_classification::alpha(),
        scalar_op::STRING_IS_DIGIT => string_classification::digit(),
        scalar_op::STRING_TO_DECIMAL => string_decimal::function(),
        scalar_op::NUMERIC_IS_ALPHA => numeric_classification::alpha(),
        scalar_op::NUMERIC_IS_DIGIT => numeric_classification::digit(),
        scalar_op::NUMERIC_TO_DECIMAL => numeric_classification::decimal(),
        scalar_op::BOOLEAN_TO_LOWERCASE_STRING => boolean_string::function(),
        scalar_op::SIGN => sign::function(),
        scalar_op::ASCII => ascii::function(),
        scalar_op::CHR => chr::function(),
        scalar_op::STARTS_WITH => datafusion::functions::string::starts_with()
            .as_ref()
            .clone(),
        scalar_op::ENDS_WITH => datafusion::functions::string::ends_with().as_ref().clone(),
        scalar_op::BINARY_STARTS_WITH => binary_boundary::starts_with(),
        scalar_op::BINARY_ENDS_WITH => binary_boundary::ends_with(),
        scalar_op::STRPOS => datafusion::functions::unicode::strpos().as_ref().clone(),
        scalar_op::LOCATE => ScalarUDF::new_from_impl(locate::FlinkLocate::new()),
        scalar_op::BIN => udf(
            "flink_bin",
            vec![DataType::Int64],
            DataType::Utf8,
            binary_strings::bin,
        ),
        scalar_op::HEX_INTEGER => udf(
            "flink_hex_int",
            vec![DataType::Int64],
            DataType::Utf8,
            binary_strings::hex_int,
        ),
        scalar_op::HEX_STRING => udf(
            "flink_hex_string",
            vec![DataType::Utf8],
            DataType::Utf8,
            |args| binary_strings::encode(args, false),
        ),
        scalar_op::TO_BASE64 => udf(
            "flink_to_base64",
            vec![DataType::Utf8],
            DataType::Utf8,
            |args| binary_strings::encode(args, true),
        ),
        scalar_op::UNHEX => udf(
            "flink_unhex",
            vec![DataType::Utf8],
            DataType::Binary,
            binary_strings::unhex,
        ),
        scalar_op::GREATEST => scalar::extremum(true),
        scalar_op::LEAST => scalar::extremum(false),
        scalar_op::INITCAP => udf(
            "flink_initcap",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::initcap,
        ),
        scalar_op::TRANSLATE => udf(
            "flink_translate",
            vec![DataType::Utf8; 3],
            DataType::Utf8,
            scalar::translate,
        ),
        scalar_op::TRIM => datafusion::functions::string::btrim().as_ref().clone(),
        scalar_op::ELT => scalar::elt_function(arity),
        scalar_op::URL_ENCODE => udf(
            "flink_url_encode",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_encode,
        ),
        scalar_op::OVERLAY => {
            let mut types = vec![DataType::Int64; arity];
            for datatype in types.iter_mut().take(2) {
                *datatype = DataType::Utf8;
            }
            udf("flink_overlay", types, DataType::Utf8, scalar::overlay)
        }
        scalar_op::URL_DECODE => udf(
            "flink_url_decode",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_decode,
        ),
        scalar_op::URL_DECODE_ASCII => udf(
            "flink_url_decode_ascii",
            vec![DataType::Utf8],
            DataType::Utf8,
            scalar::url_decode_ascii,
        ),
        scalar_op::ENCODE => encode::function(),
        scalar_op::DECODE => decode::function(),
        scalar_op::JSON_QUOTE => json_quote::function(),
        scalar_op::JSON_UNQUOTE => json_unquote::function(),
        scalar_op::SPLIT => split::function(),
        scalar_op::SUBSTRING => substring::function(arity),
        scalar_op::LEFT => left::function(),
        scalar_op::RIGHT => right::function(),
        scalar_op::LPAD => lpad::function(),
        scalar_op::RPAD => rpad::function(),
        scalar_op::SPLIT_INDEX => split_index::function(),
        scalar_op::TO_DATE => to_date::function(),
        scalar_op::QUARTER => calendar::function(calendar::Field::Quarter),
        scalar_op::WEEK => calendar::function(calendar::Field::Week),
        scalar_op::DAY_OF_YEAR => calendar::function(calendar::Field::DayOfYear),
        scalar_op::DAY_OF_WEEK => calendar::function(calendar::Field::DayOfWeek),
        scalar_op::LTRIM => datafusion::functions::string::ltrim().as_ref().clone(),
        scalar_op::RTRIM => datafusion::functions::string::rtrim().as_ref().clone(),
        scalar_op::JSON_VALUE => json_value::function(),
        scalar_op::JSON_EXISTS => json_exists::function(),
        scalar_op::IS_JSON_VALUE => is_json::function(is_json::JsonType::Value),
        scalar_op::IS_JSON_OBJECT => is_json::function(is_json::JsonType::Object),
        scalar_op::IS_JSON_ARRAY => is_json::function(is_json::JsonType::Array),
        scalar_op::IS_JSON_SCALAR => is_json::function(is_json::JsonType::Scalar),
        scalar_op::JSON_VALUE_BOOLEAN => {
            json_value::typed_function(json_value::ReturnType::Boolean)
        }
        scalar_op::JSON_VALUE_INTEGER => {
            json_value::typed_function(json_value::ReturnType::Integer)
        }
        scalar_op::JSON_VALUE_DOUBLE => json_value::typed_function(json_value::ReturnType::Double),
        scalar_op::TO_BASE64_BINARY => binary_strings::base64_binary(),
        scalar_op::JSON_SERIALIZE => json_serialize::function(),
        scalar_op::JSON_OBJECT => json_object::function(),
        scalar_op::TEMPORAL_ROUND => temporal_round::function(),
        scalar_op::TIMESTAMP_MILLIS => {
            timestamp_millis::function(timestamp_millis::Operation::Millis)
        }
        scalar_op::TIMESTAMP_SUBTRACT_MILLIS => {
            timestamp_millis::function(timestamp_millis::Operation::SubtractMillis)
        }
        scalar_op::TIMESTAMP_SUBTRACT_MONTHS => {
            timestamp_millis::function(timestamp_millis::Operation::SubtractMonths)
        }
        scalar_op::IF_NULL => if_null::function(),
        scalar_op::STRING_TO_BOOLEAN => string_to_boolean::function(false),
        scalar_op::TRY_STRING_TO_BOOLEAN => string_to_boolean::function(true),
        scalar_op::INSTR => instr::function(arity),
        scalar_op::HASH_CODE => hash_code::function(),
        scalar_op::ARRAY_DISTINCT => array_distinct::function(),
        scalar_op::UUID => datafusion::functions::string::uuid().as_ref().clone(),
        scalar_op::CARDINALITY => cardinality::function(),
        scalar_op::TYPE_OF => type_of::function(),
        scalar_op::ARRAY_REVERSE => array_copy::reverse(),
        scalar_op::ARRAY_SLICE => array_slice::function(arity),
        scalar_op::ARRAY_CONSTRUCTOR => array_constructor::function(arity, false),
        scalar_op::ARRAY_CONSTRUCTOR_COMPACT_TIMESTAMP => array_constructor::function(arity, true),
        scalar_op::UNNEST_EPOCH_MILLIS => {
            unnest_temporal::function(unnest_temporal::Operation::EpochMillis)
        }
        scalar_op::UNNEST_UTC_DATE => {
            unnest_temporal::function(unnest_temporal::Operation::UtcDate)
        }
        scalar_op::UNNEST_UTC_TIME => {
            unnest_temporal::function(unnest_temporal::Operation::UtcTime)
        }
        scalar_op::UNNEST_ZONED_DATE => {
            unnest_temporal::function(unnest_temporal::Operation::ZonedDate)
        }
        scalar_op::UNNEST_ZONED_TIME => {
            unnest_temporal::function(unnest_temporal::Operation::ZonedTime)
        }
        scalar_op::ARRAY_CONTAINS => array_search::function(true),
        scalar_op::ARRAY_POSITION => array_search::function(false),
        scalar_op::ARRAY_MIN => datafusion::functions_nested::min_max::array_min_udf()
            .as_ref()
            .clone(),
        scalar_op::ARRAY_MAX => datafusion::functions_nested::min_max::array_max_udf()
            .as_ref()
            .clone(),
        scalar_op::ARRAY_APPEND => array_edit::function(false),
        scalar_op::ARRAY_PREPEND => array_edit::function(true),
        scalar_op::ARRAY_REMOVE => array_remove::function(),
        scalar_op::ARRAY_CONCAT => array_concat::function(arity),
        scalar_op::ARRAY_UNION => datafusion::functions_nested::set_ops::array_union_udf()
            .as_ref()
            .clone(),
        scalar_op::ARRAY_EXCEPT => array_set::function(false),
        scalar_op::ARRAY_INTERSECT => array_set::function(true),
        scalar_op::ARRAY_SORT_DEFAULT => datafusion::functions_nested::sort::array_sort_udf()
            .as_ref()
            .clone(),
        scalar_op::ELEMENT => element::function(false),
        scalar_op::MAP_FROM_ARRAYS => map_arrays::function(),
        scalar_op::MAP_KEYS => map_projection::function(0),
        scalar_op::MAP_VALUES => map_projection::function(1),
        scalar_op::MAP_ENTRIES => map_projection::function(2),
        scalar_op::MAP_UNION => map_union::function(arity),
        scalar_op::MAP_CONSTRUCTOR => map_constructor::function(arity),
        scalar_op::ROW_CONSTRUCTOR => datafusion::functions::core::named_struct().as_ref().clone(),
        scalar_op::OBJECT_OF => datafusion::functions::core::named_struct().as_ref().clone(),
        scalar_op::OBJECT_UPDATE => datafusion::functions::core::named_struct().as_ref().clone(),
        scalar_op::ARRAY_JOIN => array_join::function(arity),
        scalar_op::ELEMENT_LEGACY_EXCEPTION => element::function(true),
        scalar_op::JSON_VALUE_JAVA_INPUT => json_value::java_input_function(),
        scalar_op::YEAR => calendar::function(calendar::Field::Year),
        scalar_op::MONTH => calendar::function(calendar::Field::Month),
        scalar_op::DAY => calendar::function(calendar::Field::Day),
        scalar_op::HOUR => calendar::function(calendar::Field::Hour),
        scalar_op::MINUTE => calendar::function(calendar::Field::Minute),
        scalar_op::SECOND => calendar::function(calendar::Field::Second),
        scalar_op::CALENDAR_ROUND => calendar_round::function(),
        scalar_op::TIMESTAMP_ARITHMETIC_MILLIS => {
            temporal_arithmetic::function(temporal_arithmetic::Operation::TimestampMillis)
        }
        scalar_op::DATE_ARITHMETIC_MILLIS => {
            temporal_arithmetic::function(temporal_arithmetic::Operation::DateMillis)
        }
        scalar_op::TIME_ARITHMETIC_MILLIS => {
            temporal_arithmetic::function(temporal_arithmetic::Operation::TimeMillis)
        }
        scalar_op::DATE_ARITHMETIC_MONTHS => {
            temporal_arithmetic::function(temporal_arithmetic::Operation::DateMonths)
        }
        scalar_op::TIMESTAMP_ARITHMETIC_MONTHS => {
            temporal_arithmetic::function(temporal_arithmetic::Operation::TimestampMonths)
        }
        scalar_op::TEMPORAL_GREATEST => temporal_extremum::function(arity, true),
        scalar_op::TEMPORAL_LEAST => temporal_extremum::function(arity, false),
        scalar_op::ARRAY_SORT => array_sort::function(arity),
        scalar_op::ABS_INTEGER => abs::function(),
        scalar_op::NEGATE_INTEGER => negate::function(),
        scalar_op::ARRAY_UNION_STRUCTURAL => array_set::union(),
        scalar_op::BINARY_ROW_CONSTRUCTOR => row_constructor::function(true),
        scalar_op::OBJECT_CONSTRUCTOR => row_constructor::function(false),
        scalar_op::COLLECTION_STRING_SPLIT => collection_string::split(),
        scalar_op::ARRAY_TIMSORT => array_timsort::function(arity),
        scalar_op::MAP_CONSTRUCTOR_BINARY => map_constructor::binary_function(arity),
        scalar_op::COLLECTION_STRING_LITERAL => collection_string::literal(),
        scalar_op::COLLECTION_STRING_MAP_INPUT => collection_string::map_input(),
        scalar_op::STRING_ARRAY_MIN => collection_string::extremum(true),
        scalar_op::STRING_ARRAY_MAX => collection_string::extremum(false),
        scalar_op::ROUND_INTEGER => integral_round::function(false),
        scalar_op::TRUNCATE_INTEGER => integral_round::function(true),
        scalar_op::FROM_BASE64 => from_base64::function(),
        scalar_op::BINARY_CHARACTER => binary_character::function(),
        scalar_op::UNNEST_EPOCH_SECONDS => {
            unnest_temporal::function(unnest_temporal::Operation::EpochSeconds)
        }
        scalar_op::UTC_LTZ_CEIL => temporal_round::utc_ltz_ceil(),
        scalar_op::ROUND_POSITION => round_position::function(),
        scalar_op::PRINTF_INTEGER => integer_printf::function(),
        scalar_op::UNNEST_EPOCH_PRECISION => {
            unnest_temporal::function(unnest_temporal::Operation::EpochPrecision)
        }
        scalar_op::PRINTF_BOOLEAN_ARGUMENTS => integer_printf::boolean_arguments(),
        scalar_op::DECIMAL_EPOCH_MILLIS => decimal_epoch::function(false),
        scalar_op::DECIMAL_EPOCH_SECONDS => decimal_epoch::function(true),
        scalar_op::JSON_ARRAY => json_array::function(),
        scalar_op::JSON_QUERY => json_query::function(),
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
