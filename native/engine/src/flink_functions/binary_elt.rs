use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_int32_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(crate) fn function(arity: usize, width: i32) -> ScalarUDF {
    ScalarUDF::new_from_impl(BinaryElt {
        width,
        signature: Signature::any(arity, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryElt {
    width: i32,
    signature: Signature,
}

impl ScalarUDFImpl for BinaryElt {
    fn name(&self) -> &str {
        "flink_binary_elt"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if self.width > 0 {
            DataType::FixedSizeBinary(self.width)
        } else {
            DataType::Binary
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use datafusion::logical_expr::function::Hint;
        if let Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(index)))) = args.args.first() {
            if *index > 0 {
                if let Some(ColumnarValue::Array(selected)) = args.args.get(*index as usize) {
                    let output_type = if self.width > 0 {
                        DataType::FixedSizeBinary(self.width)
                    } else {
                        DataType::Binary
                    };
                    if selected.data_type() == &output_type
                        && args.args.iter().skip(1).all(|value| {
                            matches!(
                                value.data_type(),
                                DataType::Binary | DataType::FixedSizeBinary(_)
                            ) && match value {
                                ColumnarValue::Array(array) => array.len() == args.number_rows,
                                ColumnarValue::Scalar(_) => true,
                            }
                        })
                    {
                        return Ok(ColumnarValue::Array(Arc::clone(selected)));
                    }
                }
            }
        }
        let hints = if args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Array(_)))
            && args
                .args
                .iter()
                .skip(1)
                .any(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            (0..args.args.len())
                .map(|index| {
                    if index == 0 {
                        Hint::Pad
                    } else {
                        Hint::AcceptsSingular
                    }
                })
                .collect()
        } else {
            vec![]
        };
        datafusion::functions::utils::make_scalar_function(
            |arrays| select_values(arrays, self.width, &args.args),
            hints,
        )(&args.args)
    }
}

#[cfg(test)]
fn select(args: &[ArrayRef], width: i32) -> Result<ArrayRef> {
    select_values(args, width, &[])
}

fn select_values(args: &[ArrayRef], width: i32, original: &[ColumnarValue]) -> Result<ArrayRef> {
    let Some((index, values)) = args.split_first() else {
        return exec_err!("ELT requires an index");
    };
    let indexes = as_int32_array(index)?;
    let arrays = values
        .iter()
        .map(|value| match value.data_type() {
            DataType::Binary | DataType::FixedSizeBinary(_) => Ok(Arc::clone(value)),
            _ => arrow::compute::cast(value, &DataType::Binary),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let arrays = arrays
        .iter()
        .enumerate()
        .map(|(index, array)| {
            let input = match array.data_type() {
                DataType::FixedSizeBinary(_) => BinaryInput::Fixed(
                    array
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap(),
                ),
                _ => BinaryInput::Variable(array.as_any().downcast_ref::<BinaryArray>().unwrap()),
            };
            if matches!(original.get(index + 1), Some(ColumnarValue::Scalar(_))) {
                BinaryInput::Scalar(input.value(0))
            } else {
                input
            }
        })
        .collect::<Vec<_>>();
    if width > 0 {
        let mut output = FixedSizeBinaryBuilder::with_capacity(indexes.len(), width);
        for (row, index) in indexes.iter().enumerate() {
            let selected = index
                .filter(|&index| index > 0)
                .and_then(|index| arrays.get(index as usize - 1))
                .and_then(|array| array.value(row));
            match selected {
                Some(value) => output.append_value(value)?,
                None => output.append_null(),
            }
        }
        return Ok(Arc::new(output.finish()));
    }
    let mut output = BinaryBuilder::new();
    for (row, index) in indexes.iter().enumerate() {
        let selected = index
            .filter(|&index| index > 0)
            .and_then(|index| arrays.get(index as usize - 1));
        match selected {
            Some(array) => output.append_option(array.value(row)),
            _ => output.append_null(),
        }
    }
    Ok(Arc::new(output.finish()))
}

enum BinaryInput<'a> {
    Fixed(&'a FixedSizeBinaryArray),
    Variable(&'a BinaryArray),
    Scalar(Option<&'a [u8]>),
}

impl<'a> BinaryInput<'a> {
    fn value(&self, row: usize) -> Option<&'a [u8]> {
        match self {
            Self::Fixed(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Variable(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Scalar(value) => *value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use datafusion::common::{config::ConfigOptions, ScalarValue};

    #[test]
    fn broadcasts_literals_without_turning_empty_or_single_row_arrays_into_scalars() {
        for rows in [0, 1] {
            for width in [0, 2] {
                for index in [Some(1), Some(2), None] {
                    for array_index in [false, true] {
                        for literal_null in [false, true] {
                            let source: ArrayRef =
                                Arc::new(BinaryArray::from(vec![Some(&[0xff, 0][..])]));
                            let index_arg = if array_index {
                                ColumnarValue::Array(Arc::new(Int32Array::from(vec![index; rows])))
                            } else {
                                ColumnarValue::Scalar(ScalarValue::Int32(index))
                            };
                            let literal = (!literal_null).then(|| vec![0, 0x80]);
                            let args = vec![
                                index_arg,
                                ColumnarValue::Array(source.slice(0, rows)),
                                ColumnarValue::Scalar(ScalarValue::Binary(literal.clone())),
                            ];
                            let fields = args
                                .iter()
                                .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                                .collect();
                            let output_type = if width > 0 {
                                DataType::FixedSizeBinary(width)
                            } else {
                                DataType::Binary
                            };
                            let output = function(args.len(), width)
                                .invoke_with_args(ScalarFunctionArgs {
                                    args,
                                    arg_fields: fields,
                                    number_rows: rows,
                                    return_field: Arc::new(Field::new(
                                        "output",
                                        output_type.clone(),
                                        true,
                                    )),
                                    config_options: Arc::new(ConfigOptions::new()),
                                })
                                .unwrap();
                            let ColumnarValue::Array(output) = output else {
                                panic!("an array input must retain array output representation");
                            };
                            assert_eq!(output.len(), rows);
                            assert_eq!(output.data_type(), &output_type);
                            if rows == 1 {
                                let expected = match index {
                                    Some(1) => Some(vec![0xff, 0]),
                                    Some(2) => literal,
                                    _ => None,
                                };
                                assert_eq!(
                                    ScalarValue::try_from_array(&output, 0).unwrap(),
                                    if width > 0 {
                                        ScalarValue::FixedSizeBinary(width, expected)
                                    } else {
                                        ScalarValue::Binary(expected)
                                    }
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_selection_keeps_sliced_buffers_owned_after_inputs_drop() {
        for width in [0, 2] {
            for rows in [0, 1, 3] {
                for selected_index in [1, 2] {
                    let source: ArrayRef = if width == 0 {
                        Arc::new(BinaryArray::from(vec![
                            Some(&[9, 9][..]),
                            None,
                            Some(&[0xff, 0][..]),
                            Some(&[0, 0x80][..]),
                        ]))
                    } else {
                        let mut builder = FixedSizeBinaryBuilder::new(width);
                        builder.append_value([9, 9]).unwrap();
                        builder.append_null();
                        builder.append_value([0xff, 0]).unwrap();
                        builder.append_value([0, 0x80]).unwrap();
                        Arc::new(builder.finish())
                    };
                    let input = source.slice(1, rows);
                    let expected = (0..rows)
                        .map(|row| ScalarValue::try_from_array(&input, row).unwrap())
                        .collect::<Vec<_>>();
                    let values = if selected_index == 1 {
                        vec![
                            ColumnarValue::Array(input.clone()),
                            ColumnarValue::Scalar(ScalarValue::Binary(None)),
                        ]
                    } else {
                        vec![
                            ColumnarValue::Scalar(ScalarValue::Binary(None)),
                            ColumnarValue::Array(input.clone()),
                        ]
                    };
                    let args = std::iter::once(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                        selected_index,
                    ))))
                    .chain(values)
                    .collect::<Vec<_>>();
                    let fields = args
                        .iter()
                        .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                        .collect();
                    let output = function(3, width)
                        .invoke_with_args(ScalarFunctionArgs {
                            args,
                            arg_fields: fields,
                            number_rows: rows,
                            return_field: Arc::new(Field::new(
                                "result",
                                input.data_type().clone(),
                                true,
                            )),
                            config_options: Arc::new(ConfigOptions::new()),
                        })
                        .unwrap();
                    let ColumnarValue::Array(output) = output else {
                        panic!("array result required")
                    };
                    assert!(Arc::ptr_eq(&input, &output));
                    drop(input);
                    drop(source);
                    assert_eq!(output.len(), rows);
                    for (row, expected) in expected.into_iter().enumerate() {
                        assert_eq!(ScalarValue::try_from_array(&output, row).unwrap(), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn selects_sliced_fixed_and_variable_inputs_without_padding_changes() {
        use arrow::array::FixedSizeBinaryBuilder;
        let mut fixed = FixedSizeBinaryBuilder::new(2);
        fixed.append_value([9, 9]).unwrap();
        fixed.append_value([0xff, 0]).unwrap();
        fixed.append_null();
        fixed.append_value([0, 0x80]).unwrap();
        fixed.append_value([8, 8]).unwrap();
        let fixed: ArrayRef = Arc::new(fixed.finish());
        let variable: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[9][..]),
            Some(&[][..]),
            Some(&[1, 2, 3][..]),
            None,
            Some(&[8][..]),
        ]));
        let indexes: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 1, 2, 0]));
        assert_eq!(
            select(
                &[indexes.slice(1, 3), fixed.slice(1, 3), variable.slice(1, 3)],
                0
            )
            .unwrap()
            .as_ref(),
            &BinaryArray::from(vec![Some(&[0xff, 0][..]), None, None])
        );
        let indexes: ArrayRef = Arc::new(Int32Array::from(vec![2, 2, 1]));
        assert_eq!(
            select(&[indexes, fixed.slice(1, 3), variable.slice(1, 3)], 0)
                .unwrap()
                .as_ref(),
            &BinaryArray::from(vec![
                Some(&[][..]),
                Some(&[1, 2, 3][..]),
                Some(&[0, 0x80][..])
            ])
        );
    }

    #[test]
    fn selects_one_based_indexes_and_preserves_selected_nulls() {
        let indexes: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(0),
            Some(-1),
            Some(3),
            None,
            Some(2),
        ]));
        let a: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0xff, 0][..]); 7]));
        let b: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[0, 0x80][..]),
            Some(&[0, 0x80][..]),
            None,
            None,
            None,
            None,
            None,
        ]));
        assert_eq!(
            select(&[indexes, a, b], 0).unwrap().as_ref(),
            &BinaryArray::from(vec![
                Some(&[0xff, 0][..]),
                Some(&[0, 0x80][..]),
                None,
                None,
                None,
                None,
                None
            ])
        );
    }
}
