use crate::expression_protocol::scalar_op;
use arrow::array::{
    builder::BooleanBufferBuilder, Array, ArrayRef, BinaryArray, BooleanArray, StringArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use std::{cmp::Ordering, sync::Arc};
use streamfusion_bridge::string_cache::JavaStringCacheColumn;

pub(crate) fn function(op: i64, types: Vec<DataType>) -> ScalarUDF {
    let kernel: fn(&[ArrayRef]) -> Result<ArrayRef> = match op {
        scalar_op::GREATER_THAN => compare::<{ scalar_op::GREATER_THAN }>,
        scalar_op::GREATER_OR_EQUAL => compare::<{ scalar_op::GREATER_OR_EQUAL }>,
        scalar_op::LESS_THAN => compare::<{ scalar_op::LESS_THAN }>,
        scalar_op::LESS_OR_EQUAL => compare::<{ scalar_op::LESS_OR_EQUAL }>,
        scalar_op::EQUAL => compare::<{ scalar_op::EQUAL }>,
        scalar_op::NOT_EQUAL => compare::<{ scalar_op::NOT_EQUAL }>,
        _ => unreachable!("Unknown STRING comparison {op}"),
    };
    datafusion::logical_expr::create_udf(
        &format!("flink_string_comparison_{op}"),
        types,
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(datafusion::functions::utils::make_scalar_function(
            kernel,
            vec![],
        )),
    )
}

fn bytes(value: &ArrayRef) -> Result<BinaryArray> {
    if let Some(binary) = value.as_any().downcast_ref::<BinaryArray>() {
        return Ok(binary.clone());
    }
    if let Some(text) = value.as_any().downcast_ref::<StringArray>() {
        // Offsets and validity already describe the same bytes.
        return Ok(BinaryArray::new(
            text.offsets().clone(),
            text.values().clone(),
            text.nulls().cloned(),
        ));
    }
    exec_err!(
        "Expected byte-backed or UTF-8 STRING, got {}",
        value.data_type()
    )
}

fn compare<const OP: i64>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = bytes(&args[0])?;
    let right = bytes(&args[1])?;
    let nulls = (args.len() == 4)
        .then(|| NullBuffer::union(left.nulls(), right.nulls()))
        .flatten();
    let cached_pairs = if args.len() == 4 {
        count_cached_pairs(&args[2], &args[3], nulls.as_ref())
    } else {
        0
    };
    if cached_pairs == 0 {
        let result = match OP {
            scalar_op::GREATER_THAN => cmp::gt(&left, &right),
            scalar_op::GREATER_OR_EQUAL => cmp::gt_eq(&left, &right),
            scalar_op::LESS_THAN => cmp::lt(&left, &right),
            scalar_op::LESS_OR_EQUAL => cmp::lt_eq(&left, &right),
            scalar_op::EQUAL => cmp::eq(&left, &right),
            scalar_op::NOT_EQUAL => cmp::neq(&left, &right),
            _ => unreachable!(),
        }?;
        return Ok(Arc::new(result));
    }

    let left_cache = JavaStringCacheColumn::new(&args[2]);
    let right_cache = JavaStringCacheColumn::new(&args[3]);
    let mut result = BooleanBufferBuilder::new(left.len());
    if cached_pairs == left.len() - nulls.as_ref().map_or(0, NullBuffer::null_count) {
        // Every pair with a non-null result has both caches.
        for row in 0..left.len() {
            result.append(compare_units::<OP>(
                left_cache.units_at(row),
                right_cache.units_at(row),
            ));
        }
    } else {
        let left_validity = args[2].nulls();
        let right_validity = args[3].nulls();
        for row in 0..left.len() {
            let value = if left_validity.is_none_or(|valid| valid.is_valid(row))
                && right_validity.is_none_or(|valid| valid.is_valid(row))
            {
                compare_units::<OP>(left_cache.units_at(row), right_cache.units_at(row))
            } else {
                let (left, right) = (left.value(row), right.value(row));
                match OP {
                    scalar_op::EQUAL => left == right,
                    scalar_op::NOT_EQUAL => left != right,
                    _ => test_ordering::<OP>(left.cmp(right)),
                }
            };
            result.append(value);
        }
    }
    Ok(Arc::new(BooleanArray::new(result.finish(), nulls)))
}

fn compare_units<const OP: i64>(left: &[u16], right: &[u16]) -> bool {
    match OP {
        scalar_op::EQUAL => left == right,
        scalar_op::NOT_EQUAL => left != right,
        _ => test_ordering::<OP>(utf16_cmp(left, right)),
    }
}

fn test_ordering<const OP: i64>(order: Ordering) -> bool {
    match OP {
        scalar_op::GREATER_THAN => order.is_gt(),
        scalar_op::GREATER_OR_EQUAL => order.is_ge(),
        scalar_op::LESS_THAN => order.is_lt(),
        scalar_op::LESS_OR_EQUAL => order.is_le(),
        _ => unreachable!(),
    }
}

fn count_cached_pairs(left: &ArrayRef, right: &ArrayRef, nulls: Option<&NullBuffer>) -> usize {
    let chunks =
        [left.nulls(), right.nulls(), nulls].map(|mask| mask.map(|mask| mask.inner().bit_chunks()));
    let mut masks = chunks
        .each_ref()
        .map(|chunks| chunks.as_ref().map(|chunks| chunks.iter_padded()));
    if masks.iter().all(Option::is_none) {
        return left.len();
    }
    (0..left.len().div_ceil(64))
        .map(|_| {
            let valid = masks
                .iter_mut()
                .flatten()
                .fold(u64::MAX, |bits, mask| bits & mask.next().unwrap());
            valid.count_ones() as usize
        })
        .sum()
}

fn utf16_cmp(left: &[u16], right: &[u16]) -> Ordering {
    let left_blocks = left.chunks_exact(8);
    let right_blocks = right.chunks_exact(8);
    let common_blocks = left_blocks.len().min(right_blocks.len());
    for (a, b) in left_blocks.zip(right_blocks) {
        if a != b {
            return a.cmp(b);
        }
    }
    left[common_blocks * 8..].cmp(&right[common_blocks * 8..])
}
