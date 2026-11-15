use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn starts_with() -> ScalarUDF {
    function(false)
}

pub(super) fn ends_with() -> ScalarUDF {
    function(true)
}

fn function(suffix: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(BinaryBoundary {
        suffix,
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryBoundary {
    suffix: bool,
    signature: Signature,
}

impl ScalarUDFImpl for BinaryBoundary {
    fn name(&self) -> &str {
        if self.suffix {
            "flink_binary_ends_with"
        } else {
            "flink_binary_starts_with"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, inputs: &[DataType]) -> Result<DataType> {
        if inputs.len() == 2
            && inputs
                .iter()
                .all(|t| matches!(t, DataType::Binary | DataType::FixedSizeBinary(_)))
        {
            Ok(DataType::Boolean)
        } else {
            exec_err!("Binary prefix/suffix comparison expects two binary arguments")
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        compare(self.suffix, &args.args)
    }
}

fn binary_array(value: &ColumnarValue) -> Result<ArrayRef> {
    let array = match value {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(value) => value.to_array_of_size(1)?,
    };
    match array.data_type() {
        DataType::Binary => Ok(array),
        DataType::FixedSizeBinary(width) => {
            let fixed = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let width = *width as usize;
            if fixed.values().len() > i32::MAX as usize {
                return exec_err!("Binary comparison exceeds Arrow offset capacity");
            }
            // Unlike Arrow's cast, this creates offsets without copying the fixed-width bytes.
            let offsets = (0..=fixed.len())
                .map(|i| (i * width) as i32)
                .collect::<Vec<_>>();
            Ok(Arc::new(BinaryArray::new(
                OffsetBuffer::new(offsets.into()),
                fixed.values().clone(),
                fixed.nulls().cloned(),
            )))
        }
        _ => exec_err!("Binary prefix/suffix comparison expects two binary arguments"),
    }
}

fn compare(suffix: bool, args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [left, right] = args else {
        return exec_err!("Binary prefix/suffix comparison expects two binary arguments");
    };
    let a = binary_array(left)?;
    let b = binary_array(right)?;
    let a_scalar = matches!(left, ColumnarValue::Scalar(_));
    let b_scalar = matches!(right, ColumnarValue::Scalar(_));
    let len = if a_scalar { b.len() } else { a.len() };
    if !a_scalar && !b_scalar && a.len() != b.len() {
        return exec_err!("Binary comparison arrays have different lengths");
    }
    let result = if a_scalar && a.is_null(0) || b_scalar && b.is_null(0) {
        BooleanArray::new_null(len)
    } else {
        let a = a.as_any().downcast_ref::<BinaryArray>().unwrap();
        let b = b.as_any().downcast_ref::<BinaryArray>().unwrap();
        // Byte comparisons cannot fail. Compute their bitmap independently of validity instead
        // of branching on both inputs' NULL bits for every row as Arrow's generic matcher does.
        let values = BooleanBuffer::collect_bool(len, |i| {
            let value = a.value(if a_scalar { 0 } else { i });
            let needle = b.value(if b_scalar { 0 } else { i });
            if suffix {
                value.ends_with(needle)
            } else {
                value.starts_with(needle)
            }
        });
        BooleanArray::new(
            values,
            NullBuffer::union(
                if a_scalar { None } else { a.nulls() },
                if b_scalar { None } else { b.nulls() },
            ),
        )
    };
    if matches!(
        (left, right),
        (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
    ) {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, BooleanArray};

    #[test]
    fn suffix_comparison_preserves_every_byte_slices_and_scalar_nulls() {
        let values: Vec<_> = (0..256)
            .map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some(vec![97, i as u8, 0, 255])
                }
            })
            .collect();
        let needles: Vec<_> = (0..256)
            .map(|i| match i % 6 {
                0 => None,
                1 => Some(vec![]),
                2 => Some(vec![0, 255]),
                3 => Some(vec![255, 0]),
                4 => Some(vec![97, i as u8, 0, 255, 0]),
                _ => Some(vec![i as u8, 0, 255]),
            })
            .collect();
        let left: ArrayRef = Arc::new(BinaryArray::from_iter(values.clone()));
        let right: ArrayRef = Arc::new(BinaryArray::from_iter(needles.clone()));
        for offset in 0..8 {
            for length in [0, 1, 63, 64, 65, 127, 128, 129] {
                for suffix in [false, true] {
                    let ColumnarValue::Array(result) = compare(
                        suffix,
                        &[
                            ColumnarValue::Array(left.slice(offset, length)),
                            ColumnarValue::Array(right.slice(offset, length)),
                        ],
                    )
                    .unwrap() else {
                        panic!("expected array");
                    };
                    let expected = BooleanArray::from_iter((offset..offset + length).map(|i| {
                        Some(if suffix {
                            values[i].as_ref()?.ends_with(needles[i].as_ref()?)
                        } else {
                            values[i].as_ref()?.starts_with(needles[i].as_ref()?)
                        })
                    }));
                    assert_eq!(
                        result.as_any().downcast_ref::<BooleanArray>().unwrap(),
                        &expected
                    );
                    result.to_data().validate_full().unwrap();
                }
            }
        }
        let fixed = arrow::compute::cast(&left, &DataType::FixedSizeBinary(4)).unwrap();
        let ColumnarValue::Array(result) = compare(
            true,
            &[
                ColumnarValue::Array(fixed.slice(1, 65)),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![0, 255]))),
            ],
        )
        .unwrap() else {
            panic!("expected array");
        };
        assert_eq!(
            result.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from_iter(values[1..66].iter().map(|v| v.as_ref().map(|_| true)))
        );
        for (a, b) in [
            (None, Some(vec![])),
            (Some(vec![255, 0]), Some(vec![0])),
            (Some(vec![]), Some(vec![])),
        ] {
            let ColumnarValue::Scalar(result) = compare(
                true,
                &[
                    ColumnarValue::Scalar(ScalarValue::Binary(a.clone())),
                    ColumnarValue::Scalar(ScalarValue::Binary(b.clone())),
                ],
            )
            .unwrap() else {
                panic!("expected scalar");
            };
            assert_eq!(
                result,
                ScalarValue::Boolean(a.zip(b).map(|(a, b)| a.ends_with(&b)))
            );
        }
    }

    #[test]
    fn arbitrary_bytes_empty_needles_nulls_and_offset_arrays() {
        let left = BinaryArray::from_iter([
            Some(&b"unused"[..]),
            Some(&b"\xff\0ab"[..]),
            Some(&b""[..]),
            None,
            Some(&b"abc"[..]),
            Some(&b"a"[..]),
            Some(&b"abc"[..]),
        ])
        .slice(1, 6);
        let right = BinaryArray::from_iter([
            Some(&b"unused"[..]),
            Some(&b"\xff\0"[..]),
            Some(&b""[..]),
            Some(&b""[..]),
            None,
            Some(&b"ab"[..]),
            Some(&b"b"[..]),
        ])
        .slice(1, 6);
        let output = compare(
            false,
            &[
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
        )
        .unwrap();
        let ColumnarValue::Array(output) = output else {
            panic!("expected array");
        };
        assert_eq!(
            output.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![
                Some(true),
                Some(true),
                None,
                None,
                Some(false),
                Some(false)
            ])
        );
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn scalar_and_fixed_binary_keep_raw_bytes_and_empty_batches() {
        let variable: ArrayRef = Arc::new(BinaryArray::from_iter([
            Some(&b"\xff\0"[..]),
            Some(&b"a\0"[..]),
            None,
        ]));
        let fixed = arrow::compute::cast(&variable, &DataType::FixedSizeBinary(2)).unwrap();
        for input in [variable, fixed] {
            let ColumnarValue::Array(output) = compare(
                false,
                &[
                    ColumnarValue::Array(input.clone()),
                    ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![255]))),
                ],
            )
            .unwrap() else {
                panic!("expected array");
            };
            assert_eq!(
                output.as_any().downcast_ref::<BooleanArray>().unwrap(),
                &BooleanArray::from(vec![Some(true), Some(false), None])
            );
            let ColumnarValue::Array(empty) = compare(
                false,
                &[
                    ColumnarValue::Scalar(ScalarValue::Binary(None)),
                    ColumnarValue::Array(input.slice(0, 0)),
                ],
            )
            .unwrap() else {
                panic!("expected array");
            };
            assert_eq!(empty.len(), 0);
        }
        let ColumnarValue::Scalar(output) = compare(
            false,
            &[
                ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![255, 0]))),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![255]))),
            ],
        )
        .unwrap() else {
            panic!("expected scalar");
        };
        assert_eq!(output, ScalarValue::Boolean(Some(true)));
    }
}
