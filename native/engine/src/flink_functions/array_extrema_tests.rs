use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Int64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};

fn evaluate(op: i64, input: ColumnarValue, rows: usize) -> ColumnarValue {
    let udf = super::function(op, 1).unwrap();
    let data_type = input.data_type();
    let result_type = udf.return_type(&[data_type.clone()]).unwrap();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![input],
        arg_fields: vec![Arc::new(Field::new("array", data_type, true))],
        number_rows: rows,
        return_field: Arc::new(Field::new("result", result_type, true)),
        config_options: Arc::new(datafusion::config::ConfigOptions::default()),
    })
    .unwrap()
}

#[test]
fn sliced_parent_and_child_skip_hidden_values_and_preserve_typed_empty_output() {
    let values = Int64Array::from(vec![
        Some(999),
        Some(99),
        Some(98),
        Some(3),
        None,
        None,
        None,
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-7),
        Some(7),
    ])
    .slice(1, 10);
    let input = ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0, 2, 4, 4, 6, 8, 10].into()),
        Arc::new(values),
        Some(NullBuffer::from(vec![true, true, true, true, false, true])),
    )
    .slice(1, 5);
    for (op, winner) in [(180, -7), (181, 7)] {
        let result = evaluate(op, ColumnarValue::Array(Arc::new(input.clone())), 5)
            .into_array(5)
            .unwrap();
        assert_eq!(
            result.as_ref(),
            &Int64Array::from(vec![Some(3), None, None, None, Some(winner)])
        );
        let empty = evaluate(op, ColumnarValue::Array(Arc::new(input.slice(0, 0))), 0)
            .into_array(0)
            .unwrap();
        assert_eq!(empty.data_type(), &DataType::Int64);
        assert!(empty.is_empty());
    }
}

#[test]
fn integer_types_preserve_limits_and_nulls_across_scalar_and_arrow_kernel_thresholds() {
    for (kind, low, high) in [
        (DataType::Int8, i8::MIN as i64, i8::MAX as i64),
        (DataType::Int16, i16::MIN as i64, i16::MAX as i64),
        (DataType::Int32, i32::MIN as i64, i32::MAX as i64),
        (DataType::Int64, i64::MIN, i64::MAX),
    ] {
        for width in [0, 1, 8, 31, 32, 33, 64, 65] {
            let mut values = vec![Some(0); width];
            if width > 1 {
                values[0] = Some(low);
                values[width - 1] = Some(high);
                if width > 2 {
                    values[1] = None;
                }
            }
            let input = ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(values),
                Some(vec![None; width]),
                None,
            ]);
            let input = arrow::compute::cast(
                &input,
                &DataType::List(Arc::new(Field::new("item", kind.clone(), true))),
            )
            .unwrap();
            for (op, winner) in [(180, low), (181, high)] {
                let expected = Int64Array::from(vec![
                    if width == 0 {
                        None
                    } else if width == 1 {
                        Some(0)
                    } else {
                        Some(winner)
                    },
                    None,
                    None,
                ]);
                let expected = arrow::compute::cast(&expected, &kind).unwrap();
                let result = evaluate(op, ColumnarValue::Array(input.clone()), 3)
                    .into_array(3)
                    .unwrap();
                assert_eq!(
                    result.as_ref(),
                    expected.as_ref(),
                    "{kind:?}, width={width}, op={op}"
                );
            }
        }
    }
}

#[test]
fn decimals_keep_precision_scale_and_exact_order_for_small_and_long_arrays() {
    let limit = 10i128.pow(38) - 1;
    for scale in [0, 9, 18, 38] {
        for width in [8, 31, 32, 33, 65] {
            let mut values = vec![Some(9_007_199_254_740_993i128); width];
            values[0] = Some(-limit);
            values[1] = None;
            values[width - 1] = Some(limit);
            let child: ArrayRef = Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, scale)
                    .unwrap(),
            );
            let input = ListArray::new(
                Arc::new(Field::new("item", child.data_type().clone(), true)),
                OffsetBuffer::new(vec![0, width as i32, width as i32].into()),
                child,
                None,
            );
            for (op, winner) in [(180, -limit), (181, limit)] {
                let result = evaluate(op, ColumnarValue::Array(Arc::new(input.clone())), 2)
                    .into_array(2)
                    .unwrap();
                let expected = Decimal128Array::from(vec![Some(winner), None])
                    .with_precision_and_scale(38, scale)
                    .unwrap();
                assert_eq!(result.as_ref(), &expected);
            }
        }
    }
}

#[test]
fn scalar_arrays_return_scalar_results_with_zero_batch_rows() {
    let list = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
        vec![Some(3), None, Some(-1)],
    )]));
    for (op, expected) in [(180, -1), (181, 3)] {
        let result = evaluate(
            op,
            ColumnarValue::Scalar(ScalarValue::List(list.clone())),
            0,
        );
        assert!(
            matches!(result, ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) if value == expected)
        );
    }
}
