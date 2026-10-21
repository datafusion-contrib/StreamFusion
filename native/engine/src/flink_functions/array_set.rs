use std::hash::Hash;
use std::sync::Arc;

use ahash::AHashSet;
use arrow::array::{Array, ArrayRef, BooleanArray, ListArray, PrimitiveArray, UInt32Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(intersect: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArraySet {
        signature: Signature::any(2, Volatility::Immutable),
        intersect,
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArraySet {
    signature: Signature,
    intersect: bool,
}

impl ScalarUDFImpl for ArraySet {
    fn name(&self) -> &str {
        if self.intersect {
            "flink_array_intersect"
        } else {
            "flink_array_except"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [list @ DataType::List(l), DataType::List(r)] if l.data_type() == r.data_type() => {
                Ok(list.clone())
            }
            _ => exec_err!("ARRAY_EXCEPT requires matching arrays"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays| set(arrays, self.intersect),
            vec![],
        )(&args.args)
    }
}

fn set(args: &[ArrayRef], intersect: bool) -> Result<ArrayRef> {
    let left = as_list_array(&args[0])?;
    let right = as_list_array(&args[1])?;
    macro_rules! primitive {
        ($ty:ty) => {{
            let l = left
                .values()
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .unwrap();
            let r = right
                .values()
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .unwrap();
            select(
                left,
                right,
                intersect,
                |i| l.is_valid(i).then(|| l.value(i)),
                |i| r.is_valid(i).then(|| r.value(i)),
            )
        }};
    }
    match left.value_type() {
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::Decimal128(_, _) => primitive!(Decimal128Type),
        DataType::Boolean => {
            let l = left
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let r = right
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            select(
                left,
                right,
                intersect,
                |i| l.is_valid(i).then(|| l.value(i)),
                |i| r.is_valid(i).then(|| r.value(i)),
            )
        }
        _ => exec_err!("ARRAY_EXCEPT requires matching basic arrays"),
    }
}

fn select<K: Eq + Hash + Copy>(
    left: &ListArray,
    right: &ListArray,
    intersect: bool,
    l: impl Fn(usize) -> Option<K>,
    r: impl Fn(usize) -> Option<K>,
) -> Result<ArrayRef> {
    let mut seen = AHashSet::new();
    let mut indices = Vec::with_capacity(left.values().len());
    let mut offsets = Vec::with_capacity(left.len() + 1);
    offsets.push(0);
    for row in 0..left.len() {
        if left.is_valid(row) && right.is_valid(row) {
            seen.clear();
            for i in right.value_offsets()[row] as usize..right.value_offsets()[row + 1] as usize {
                seen.insert(r(i));
            }
            for i in left.value_offsets()[row] as usize..left.value_offsets()[row + 1] as usize {
                if if intersect {
                    seen.remove(&l(i))
                } else {
                    seen.insert(l(i))
                } {
                    indices.push(i as u32);
                }
            }
        }
        offsets.push(indices.len() as i32);
    }
    let DataType::List(field) = left.data_type() else {
        unreachable!()
    };
    Ok(Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        arrow::compute::take(left.values().as_ref(), &UInt32Array::from(indices), None)?,
        NullBuffer::union(left.nulls(), right.nulls()),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn except_is_distinct_null_safe_and_keeps_left_order() {
        let left: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(99)]),
                Some(vec![Some(3), None, Some(1), Some(3), None, Some(2)]),
                Some(vec![None, Some(1), None, Some(2)]),
                Some(vec![]),
                None,
                Some(vec![Some(1)]),
            ])
            .slice(1, 5),
        );
        let right: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(99)]),
                Some(vec![Some(2)]),
                Some(vec![None]),
                Some(vec![]),
                Some(vec![]),
                None,
            ])
            .slice(1, 5),
        );
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(3), None, Some(1)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
            None,
            None,
        ]);
        assert_eq!(
            set(&[left.clone(), right.clone()], false).unwrap().as_ref(),
            &expected
        );
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(2)]),
            Some(vec![None]),
            Some(vec![]),
            None,
            None,
        ]);
        assert_eq!(set(&[left, right], true).unwrap().as_ref(), &expected);
    }

    #[test]
    fn intersection_never_reorders_the_shorter_left_input() {
        let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            vec![Some(3), Some(1)],
        )]));
        let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            vec![Some(1), Some(3), Some(9)],
        )]));
        assert_eq!(
            set(&[left.clone(), right], true).unwrap().as_ref(),
            left.as_ref()
        );
    }
}
