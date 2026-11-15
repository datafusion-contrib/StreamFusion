use crate::expression_protocol::scalar_op;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::{cast::as_primitive_array, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};

pub(crate) fn comparison(op: i64, types: &[DataType]) -> Option<ScalarUDF> {
    if types.len() != 2
        || !types
            .iter()
            .any(|t| matches!(t, DataType::Float32 | DataType::Float64))
        || !types.iter().all(|t| {
            t.is_integer() || matches!(t, DataType::Float32 | DataType::Float64 | DataType::Null)
        })
    {
        return None;
    }
    // Java promotes primitive operands to double if either is double, otherwise to float.
    let datatype = if types.contains(&DataType::Float64) {
        DataType::Float64
    } else {
        DataType::Float32
    };
    Some(datafusion::logical_expr::create_udf(
        &format!("flink_float_comparison_{op}"),
        vec![datatype; 2],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(datafusion::functions::utils::make_scalar_function(
            move |args| compare(args, op),
            vec![],
        )),
    ))
}

fn compare(args: &[ArrayRef], op: i64) -> Result<ArrayRef> {
    macro_rules! compare {
        ($t:ty) => {{
            let left = as_primitive_array::<$t>(&args[0])?;
            let right = as_primitive_array::<$t>(&args[1])?;
            left.iter()
                .zip(right.iter())
                .map(|(left, right)| {
                    left.zip(right).map(|(left, right)| match op {
                        scalar_op::GREATER_THAN => left > right,
                        scalar_op::GREATER_OR_EQUAL => left >= right,
                        scalar_op::LESS_THAN => left < right,
                        scalar_op::LESS_OR_EQUAL => left <= right,
                        scalar_op::EQUAL => left == right,
                        scalar_op::NOT_EQUAL => left != right,
                        _ => unreachable!("floating comparison opcode"),
                    })
                })
                .collect::<BooleanArray>()
        }};
    }
    Ok(Arc::new(match args[0].data_type() {
        DataType::Float32 => compare!(Float32Type),
        DataType::Float64 => compare!(Float64Type),
        _ => unreachable!("floating comparison input"),
    }))
}
