use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int32Array, ListArray};
use arrow::datatypes::{DataType, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(contains: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(ArraySearch {
        contains,
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArraySearch {
    contains: bool,
    signature: Signature,
}

impl ScalarUDFImpl for ArraySearch {
    fn name(&self) -> &str {
        if self.contains {
            "flink_array_contains"
        } else {
            "flink_array_position"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::List(field), needle]
                if field.data_type() == needle
                    && matches!(
                        needle,
                        DataType::Boolean
                            | DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Decimal128(_, _)
                    ) =>
            {
                Ok(if self.contains {
                    DataType::Boolean
                } else {
                    DataType::Int32
                })
            }
            _ => exec_err!(
                "{} requires a BOOLEAN, integer or DECIMAL ARRAY and a matching element",
                self.name()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array, needle] = args.args.as_slice() else {
            return exec_err!("{} requires two arguments", self.name());
        };
        self.return_type(&[array.data_type(), needle.data_type()])?;
        let scalar_result = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let scalar_needle = matches!(needle, ColumnarValue::Scalar(_));
        let rows = if scalar_result { 1 } else { args.number_rows };
        let array = array.to_array(rows)?;
        let needle = needle.to_array(if scalar_needle { 1 } else { rows })?;
        let result = evaluate(&array, &needle, scalar_needle, self.contains)?;
        if scalar_result {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn evaluate(array: &ArrayRef, needle: &ArrayRef, scalar: bool, contains: bool) -> Result<ArrayRef> {
    let array = array.as_list::<i32>();
    let values = array.values();
    macro_rules! primitive {
        ($kind:ty) => {{
            let values = values.as_primitive::<$kind>();
            let needle = needle.as_primitive::<$kind>();
            search(
                array,
                |i| values.is_valid(i).then(|| values.value(i)),
                |i| needle.is_valid(i).then(|| needle.value(i)),
                scalar,
                contains,
            )
        }};
    }
    Ok(match values.data_type() {
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::Decimal128(_, _) => primitive!(Decimal128Type),
        DataType::Boolean => {
            let values = values.as_boolean();
            let needle = needle.as_boolean();
            search(
                array,
                |i| values.is_valid(i).then(|| values.value(i)),
                |i| needle.is_valid(i).then(|| needle.value(i)),
                scalar,
                contains,
            )
        }
        other => return exec_err!("Unsupported array search element: {other}"),
    })
}

fn search<T: PartialEq>(
    array: &ListArray,
    value: impl Fn(usize) -> Option<T>,
    needle: impl Fn(usize) -> Option<T>,
    scalar: bool,
    contains: bool,
) -> ArrayRef {
    let offsets = array.value_offsets();
    let positions = (0..array.len()).map(|row| {
        if array.is_null(row) {
            return None;
        }
        let needle = needle(if scalar { 0 } else { row });
        // Flink CONTAINS matches NULL elements; POSITION returns NULL for a NULL needle.
        if !contains && needle.is_none() {
            return None;
        }
        let first = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        Some(
            (first..end)
                .position(|i| value(i) == needle)
                .map_or(0, |i| (i + 1) as i32),
        )
    });
    if contains {
        Arc::new(BooleanArray::from_iter(
            positions.map(|value| value.map(|i| i > 0)),
        ))
    } else {
        Arc::new(Int32Array::from_iter(positions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Int32Builder, ListBuilder};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;

    #[test]
    fn sliced_inputs_keep_first_matches_null_needles_and_hidden_children() {
        let values = Int32Array::from(vec![
            Some(999),
            Some(99),
            Some(98),
            Some(1),
            None,
            None,
            Some(2),
            Some(0),
            Some(1),
            Some(1),
        ])
        .slice(1, 9);
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 2, 4, 4, 6, 7, 9].into()),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, true, true, true, false, true])),
        ));
        let needle: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(99),
            None,
            None,
            Some(2),
            Some(0),
            Some(1),
        ]));
        let array = array.slice(1, 5);
        let needle = needle.slice(1, 5);
        assert_eq!(
            evaluate(&array, &needle, false, true).unwrap().as_ref(),
            &BooleanArray::from(vec![Some(true), Some(false), Some(true), None, Some(true)])
        );
        assert_eq!(
            evaluate(&array, &needle, false, false).unwrap().as_ref(),
            &Int32Array::from(vec![None, None, Some(2), None, Some(1)])
        );
        let absent: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        assert_eq!(
            evaluate(&array, &absent, true, false).unwrap().as_ref(),
            &Int32Array::from(vec![Some(0), Some(0), Some(0), None, Some(0)])
        );
        assert_eq!(
            evaluate(&array.slice(0, 0), &needle.slice(0, 0), false, true)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn boolean_null_search_and_exact_decimal_equality() {
        let booleans: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(vec![0, 3].into()),
            Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
            None,
        ));
        let null: ArrayRef = Arc::new(BooleanArray::from(vec![None]));
        assert_eq!(
            evaluate(&booleans, &null, true, true).unwrap().as_ref(),
            &BooleanArray::from(vec![true])
        );
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(9007199254740993i128),
                None,
                Some(-99999999999999999999999999999999999999i128),
            ])
            .with_precision_and_scale(38, 9)
            .unwrap(),
        );
        let decimals: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 3].into()),
            values,
            None,
        ));
        let needle: ArrayRef = Arc::new(
            Decimal128Array::from(vec![9007199254740993i128])
                .with_precision_and_scale(38, 9)
                .unwrap(),
        );
        assert_eq!(
            evaluate(&decimals, &needle, true, false).unwrap().as_ref(),
            &Int32Array::from(vec![1])
        );
    }

    #[test]
    fn scalar_adaptation_does_not_broadcast_constant_needles() {
        let mut lists = ListBuilder::new(Int32Builder::new());
        lists.values().append_slice(&[3, 1, 3]);
        lists.append(true);
        let array = ScalarValue::List(Arc::new(lists.finish()));
        let udf = function(false);
        let types = vec![array.data_type(), DataType::Int32];
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(array),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
                ],
                arg_fields: types
                    .iter()
                    .map(|ty| Arc::new(Field::new("arg", ty.clone(), true)))
                    .collect(),
                number_rows: 0,
                return_field: Arc::new(Field::new("result", DataType::Int32, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))
        ));
        let wrong = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(20, 2),
            true,
        )));
        assert!(udf
            .return_type(&[wrong, DataType::Decimal128(20, 3)])
            .is_err());
    }
}
