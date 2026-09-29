use std::hash::Hash;
use std::sync::Arc;

use ahash::AHashSet;
use arrow::array::{Array, ArrayRef, ListArray, PrimitiveArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{cast::as_list_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(IntegerArrayDistinct {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntegerArrayDistinct {
    signature: Signature,
}

impl ScalarUDFImpl for IntegerArrayDistinct {
    fn name(&self) -> &str {
        "flink_integer_array_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [array @ DataType::List(field)]
                if matches!(
                    field.data_type(),
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ) =>
            {
                Ok(array.clone())
            }
            _ => exec_err!("ARRAY_DISTINCT requires an integer ARRAY"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(distinct, vec![])(&args.args)
    }
}

fn distinct(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("ARRAY_DISTINCT requires one argument");
    };
    let array = as_list_array(input)?;
    match array.value_type() {
        DataType::Int8 => distinct_primitive::<Int8Type>(array),
        DataType::Int16 => distinct_primitive::<Int16Type>(array),
        DataType::Int32 => distinct_primitive::<Int32Type>(array),
        DataType::Int64 => distinct_primitive::<Int64Type>(array),
        _ => exec_err!("ARRAY_DISTINCT requires an integer ARRAY"),
    }
}

fn distinct_primitive<T: ArrowPrimitiveType>(array: &ListArray) -> Result<ArrayRef>
where
    T::Native: Eq + Hash,
{
    let values = array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("integer array element type");
    let input_offsets = array.value_offsets();
    let visible = (input_offsets[array.len()] - input_offsets[0]) as usize;
    let mut indices = Vec::with_capacity(visible);
    let mut offsets = Vec::with_capacity(array.len() + 1);
    let mut seen = AHashSet::new();
    offsets.push(0);
    for row in 0..array.len() {
        if !array.is_null(row) {
            let mut seen_null = false;
            let start = input_offsets[row] as usize;
            let end = input_offsets[row + 1] as usize;
            let mut small_values = [T::Native::default(); 8];
            let mut small_len = 0;
            let mut small_fingerprint = 0u64;
            let small = end - start <= small_values.len();
            if !small {
                seen.clear();
            }
            for index in start..end {
                let first = if values.is_null(index) {
                    !std::mem::replace(&mut seen_null, true)
                } else if small {
                    let value = values.value(index);
                    let bit = 1u64 << (value.as_usize() & 63);
                    if small_fingerprint & bit != 0 && small_values[..small_len].contains(&value) {
                        false
                    } else {
                        small_fingerprint |= bit;
                        small_values[small_len] = value;
                        small_len += 1;
                        true
                    }
                } else {
                    seen.insert(values.value(index))
                };
                if first {
                    indices.push(index as u32);
                }
            }
        }
        offsets.push(indices.len() as i32);
    }
    if indices.len() == visible {
        return Ok(Arc::new(array.clone()));
    }
    let output = arrow::compute::take(array.values().as_ref(), &UInt32Array::from(indices), None)?;
    let DataType::List(field) = array.data_type() else {
        unreachable!("list array type")
    };
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        OffsetBuffer::new(offsets.into()),
        output,
        array.nulls().cloned(),
    )?))
}
