use ahash::AHashSet;
use arrow::array::{make_array, Array, ArrayRef, BooleanArray, MutableArrayData, PrimitiveArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(arity: usize) -> ScalarUDF {
    ScalarUDF::new_from_impl(MapUnion {
        signature: Signature::any(arity, Volatility::Immutable),
    })
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapUnion {
    signature: Signature,
}
impl ScalarUDFImpl for MapUnion {
    fn name(&self) -> &str {
        "flink_map_union"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let first = super::map_projection::fields(&types[0])?;
        for ty in types.iter().skip(1) {
            let fields = super::map_projection::fields(ty)?;
            if fields
                .iter()
                .zip(first)
                .any(|(a, b)| a.data_type() != b.data_type())
            {
                return exec_err!("MAP_UNION requires matching map types");
            }
        }
        Ok(super::map_arrays::map_type(
            first[0].data_type().clone(),
            first[1].data_type().clone(),
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(union, vec![])(&args.args)
    }
}
fn union(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() == 1 {
        let (entries, offsets, nulls) = super::map_projection::parts(&args[0])?;
        return super::map_arrays::build(
            entries.column(0).clone(),
            entries.column(1).clone(),
            offsets.clone(),
            nulls.cloned(),
        );
    }
    let mut result = args[0].clone();
    for right in &args[1..] {
        result = pair(&result, right)?;
    }
    Ok(result)
}
fn pair(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    let (l, _, _) = super::map_projection::parts(left)?;
    let (r, _, _) = super::map_projection::parts(right)?;
    macro_rules! primitive {
        ($ty:ty) => {{
            let lk = l
                .column(0)
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .unwrap();
            let rk = r
                .column(0)
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .unwrap();
            select(left, right, |side, i| {
                let a = if side == 0 { lk } else { rk };
                a.is_valid(i).then(|| a.value(i) as i128)
            })
        }};
    }
    match l.column(0).data_type() {
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::Decimal128(_, _) => primitive!(Decimal128Type),
        DataType::Boolean => {
            let lk = l.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
            let rk = r.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
            select(left, right, |side, i| {
                let a = if side == 0 { lk } else { rk };
                a.is_valid(i).then(|| i128::from(a.value(i)))
            })
        }
        _ => {
            exec_err!("MAP_UNION requires verified basic keys")
        }
    }
}
fn select(
    left: &ArrayRef,
    right: &ArrayRef,
    key: impl Fn(usize, usize) -> Option<i128>,
) -> Result<ArrayRef> {
    let (l, lo, ln) = super::map_projection::parts(left)?;
    let (r, ro, rn) = super::map_projection::parts(right)?;
    let kd = [l.column(0).to_data(), r.column(0).to_data()];
    let vd = [l.column(1).to_data(), r.column(1).to_data()];
    let mut keys = MutableArrayData::new(kd.iter().collect(), false, l.len() + r.len());
    let mut values = MutableArrayData::new(vd.iter().collect(), false, l.len() + r.len());
    let validity = NullBuffer::union(ln, rn);
    let mut seen = AHashSet::new();
    let mut offsets = Vec::with_capacity(left.len() + 1);
    offsets.push(0);
    for row in 0..left.len() {
        if validity.as_ref().is_none_or(|n| n.is_valid(row)) {
            let lr = lo[row] as usize..lo[row + 1] as usize;
            let rr = ro[row] as usize..ro[row + 1] as usize;
            if rr.is_empty() {
                keys.extend(0, lr.start, lr.end);
                values.extend(0, lr.start, lr.end);
            } else {
                seen.clear();
                for i in rr.clone() {
                    seen.insert(key(1, i));
                }
                keys.extend(1, rr.start, rr.end);
                values.extend(1, rr.start, rr.end);
                let right_has_null = seen.contains(&None);
                for i in lr {
                    let k = key(0, i);
                    // Flink retains all left NULL entries if the right side has no NULL key.
                    if k.is_none() && !right_has_null || k.is_some() && seen.insert(k) {
                        keys.extend(0, i, i + 1);
                        values.extend(0, i, i + 1);
                    }
                }
            }
        }
        offsets.push(keys.len() as i32);
    }
    super::map_arrays::build(
        make_array(keys.freeze()),
        make_array(values.freeze()),
        OffsetBuffer::new(offsets.into()),
        validity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::Arc;
    fn map(keys: Vec<Option<i64>>, values: Vec<Option<i64>>, offsets: Vec<i32>) -> ArrayRef {
        super::super::map_arrays::build(
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            OffsetBuffer::new(offsets.into()),
            None,
        )
        .unwrap()
    }
    #[test]
    fn unary_ordinary_maps_keep_the_declared_carrier_and_share_buffers() {
        use arrow::array::{Int64Builder, MapBuilder};
        let mut input = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        for valid in [true, false, true] {
            input.keys().append_value(1);
            input.values().append_null();
            input.keys().append_value(1);
            input.values().append_value(2);
            input.append(valid).unwrap();
        }
        let input: ArrayRef = Arc::new(input.finish().slice(1, 2));
        let output = super::union(&[input.clone()]).unwrap();
        let (a, ao, an) = super::super::map_projection::parts(&input).unwrap();
        let (b, bo, bn) = super::super::map_projection::parts(&output).unwrap();
        assert_eq!(
            output.data_type(),
            &super::super::map_arrays::map_type(DataType::Int64, DataType::Int64)
        );
        assert_eq!(ao, bo);
        assert_eq!(an, bn);
        for i in 0..2 {
            assert_eq!(
                a.column(i).to_data().buffers()[0].as_ptr(),
                b.column(i).to_data().buffers()[0].as_ptr()
            );
        }
    }

    #[test]
    fn right_order_duplicates_and_left_null_exception_match_flink() {
        let l = map(
            vec![Some(1), Some(1), None, None, Some(3), Some(1), Some(1)],
            vec![
                Some(10),
                Some(11),
                Some(12),
                Some(13),
                Some(14),
                Some(20),
                Some(21),
            ],
            vec![0, 5, 7],
        );
        let r = map(
            vec![Some(2), Some(2)],
            vec![Some(30), Some(31)],
            vec![0, 2, 2],
        );
        let expected = map(
            vec![
                Some(2),
                Some(2),
                Some(1),
                None,
                None,
                Some(3),
                Some(1),
                Some(1),
            ],
            vec![
                Some(30),
                Some(31),
                Some(10),
                Some(12),
                Some(13),
                Some(14),
                Some(20),
                Some(21),
            ],
            vec![0, 6, 8],
        );
        assert_eq!(pair(&l, &r).unwrap().as_ref(), expected.as_ref());
        assert_eq!(
            pair(&l.slice(1, 1), &r.slice(1, 1)).unwrap().as_ref(),
            expected.slice(1, 1).as_ref()
        );
        let r = map(vec![None], vec![Some(99)], vec![0, 1]);
        let expected = map(
            vec![None, Some(1), Some(3)],
            vec![Some(99), Some(10), Some(14)],
            vec![0, 3],
        );
        assert_eq!(
            pair(&l.slice(0, 1), &r).unwrap().as_ref(),
            expected.as_ref()
        );
    }
}
