use arrow::array::{Array, BinaryArray, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

enum Input<'a> {
    Scalar(&'a [u8]),
    Binary(&'a BinaryArray),
    Utf8(&'a StringArray),
}

impl Input<'_> {
    fn value(&self, row: usize) -> &[u8] {
        match self {
            Self::Scalar(bytes) => bytes,
            Self::Binary(values) => values.value(row),
            Self::Utf8(values) => values.value(row).as_bytes(),
        }
    }

    fn capacity(&self, rows: usize) -> usize {
        match self {
            Self::Scalar(bytes) => bytes.len().saturating_mul(rows),
            Self::Binary(values) => {
                let offsets = values.value_offsets();
                (offsets[values.len()] - offsets[0]) as usize
            }
            Self::Utf8(values) => {
                let offsets = values.value_offsets();
                (offsets[values.len()] - offsets[0]) as usize
            }
        }
    }
}

/// Flink CONCAT materializes bytes and discards its arguments' Java string caches.
pub(crate) fn concat(args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue> {
    let scalar = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let rows = if scalar { 1 } else { rows };
    let mut nulls = None;
    let mut inputs = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            ColumnarValue::Scalar(value) if value.is_null() => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => {
                inputs.push(Input::Scalar(value.as_bytes()));
            }
            ColumnarValue::Scalar(ScalarValue::Binary(Some(value))) => {
                inputs.push(Input::Scalar(value));
            }
            ColumnarValue::Array(array) => {
                if array.len() != rows {
                    return exec_err!("CONCAT arguments have different lengths");
                }
                nulls = NullBuffer::union(nulls.as_ref(), array.nulls());
                if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
                    inputs.push(Input::Binary(values));
                } else if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
                    inputs.push(Input::Utf8(values));
                } else {
                    return exec_err!("CONCAT expects character arguments");
                }
            }
            _ => return exec_err!("CONCAT expects character arguments"),
        }
    }
    if nulls
        .as_ref()
        .is_some_and(|nulls| nulls.null_count() == rows)
    {
        return Ok(ColumnarValue::Array(Arc::new(BinaryArray::new_null(rows))));
    }
    // Reserve an upper bound only for unmasked batches: null rows may hide large payloads.
    let capacity = if nulls.is_none() {
        inputs
            .iter()
            .fold(0usize, |sum, input| {
                sum.saturating_add(input.capacity(rows))
            })
            .min(i32::MAX as usize)
    } else {
        0
    };
    let mut bytes = Vec::with_capacity(capacity);
    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i32);
    for row in 0..rows {
        if nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row)) {
            for input in &inputs {
                let value = input.value(row);
                if value.len() > i32::MAX as usize - bytes.len() {
                    return Err(exec_datafusion_err!(
                        "CONCAT output exceeds the Binary offset limit"
                    ));
                }
                bytes.extend_from_slice(value);
            }
        }
        offsets.push(bytes.len() as i32);
    }
    if scalar {
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))));
    }
    Ok(ColumnarValue::Array(Arc::new(BinaryArray::new(
        OffsetBuffer::new(offsets.into()),
        bytes.into(),
        nulls,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_bytes_nulls_and_slices_are_preserved() {
        let input = BinaryArray::from(vec![
            Some(&b"unused"[..]),
            Some(&[0xff]),
            Some(&[0x41, 0xe4]),
            None,
            Some(&[]),
            Some(&[0]),
            Some(&[0xc3, 0xa9]),
            Some(&b"unused"[..]),
        ])
        .slice(1, 6);
        let output = concat(
            &[
                ColumnarValue::Array(Arc::new(input)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("!".into()))),
            ],
            6,
        )
        .unwrap()
        .into_array(6)
        .unwrap();
        assert_eq!(
            output.as_ref(),
            &BinaryArray::from(vec![
                Some(&[0xff, b'!'][..]),
                Some(&[0x41, 0xe4, b'!']),
                None,
                Some(&b"!"[..]),
                Some(&[0, b'!']),
                Some(&[0xc3, 0xa9, b'!']),
            ])
        );
    }

    #[test]
    fn concatenation_can_make_two_invalid_inputs_valid() {
        let output = concat(
            &[
                ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![0xc3]))),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![0xa9]))),
            ],
            4096,
        )
        .unwrap();
        match output {
            ColumnarValue::Scalar(value) => {
                assert_eq!(value, ScalarValue::Binary(Some(vec![0xc3, 0xa9])))
            }
            other => panic!("expected scalar, got {other:?}"),
        }
    }

    #[test]
    fn empty_batches_and_null_scalars_remain_empty_or_null() {
        let input = ColumnarValue::Array(Arc::new(BinaryArray::from(Vec::<&[u8]>::new())));
        let output = concat(
            &[
                input.clone(),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("!".into()))),
            ],
            0,
        )
        .unwrap()
        .into_array(0)
        .unwrap();
        assert_eq!(output.len(), 0);
        assert!(matches!(
            concat(&[input, ColumnarValue::Scalar(ScalarValue::Utf8(None))], 0).unwrap(),
            ColumnarValue::Scalar(ScalarValue::Binary(None))
        ));
    }
}
