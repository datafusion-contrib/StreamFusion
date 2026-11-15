use arrow::array::{Array, BinaryArray, NullBufferBuilder, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(ByteConcatWs {
        signature: Signature::variadic_any(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ByteConcatWs {
    signature: Signature,
}

impl ScalarUDFImpl for ByteConcatWs {
    fn name(&self) -> &str {
        "flink_concat_ws_bytes"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, inputs: &[DataType]) -> Result<DataType> {
        if inputs.is_empty()
            || inputs
                .iter()
                .any(|t| !matches!(t, DataType::Utf8 | DataType::Binary | DataType::Null))
        {
            return exec_err!("CONCAT_WS expects a separator and character arguments");
        }
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        concatenate(&args.args, args.number_rows)
    }
}

enum Input<'a> {
    Null,
    Scalar(&'a [u8]),
    Binary(&'a BinaryArray),
    Utf8(&'a StringArray),
}

impl<'a> Input<'a> {
    fn new(value: &'a ColumnarValue, rows: usize) -> Result<Self> {
        match value {
            ColumnarValue::Scalar(value) if value.is_null() => Ok(Self::Null),
            ColumnarValue::Scalar(ScalarValue::Binary(Some(value))) => Ok(Self::Scalar(value)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => {
                Ok(Self::Scalar(value.as_bytes()))
            }
            ColumnarValue::Array(array) => {
                if array.len() != rows {
                    return exec_err!("CONCAT_WS arguments have different lengths");
                }
                if let Some(value) = array.as_any().downcast_ref::<BinaryArray>() {
                    Ok(Self::Binary(value))
                } else if let Some(value) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(Self::Utf8(value))
                } else if array.data_type() == &DataType::Null {
                    Ok(Self::Null)
                } else {
                    exec_err!("CONCAT_WS expects character arguments")
                }
            }
            _ => exec_err!("CONCAT_WS expects character arguments"),
        }
    }

    fn value(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Null => None,
            Self::Scalar(value) => Some(value),
            Self::Binary(values) => values.is_valid(row).then(|| values.value(row)),
            Self::Utf8(values) => values.is_valid(row).then(|| values.value(row).as_bytes()),
        }
    }

    fn capacity(&self, rows: usize) -> Option<usize> {
        match self {
            Self::Null => None,
            Self::Scalar(value) => Some(value.len().saturating_mul(rows)),
            Self::Binary(values) if values.null_count() == 0 => {
                let offsets = values.value_offsets();
                Some((offsets[values.len()] - offsets[0]) as usize)
            }
            Self::Utf8(values) if values.null_count() == 0 => {
                let offsets = values.value_offsets();
                Some((offsets[values.len()] - offsets[0]) as usize)
            }
            _ => None,
        }
    }
}

fn append(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    if value.len() > i32::MAX as usize - output.len() {
        return exec_err!("CONCAT_WS output exceeds the Binary offset limit");
    }
    output.extend_from_slice(value);
    Ok(())
}

fn concatenate(args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue> {
    let scalar = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let rows = if scalar { 1 } else { rows };
    let inputs = args
        .iter()
        .map(|arg| Input::new(arg, rows))
        .collect::<Result<Vec<_>>>()?;
    let Some((separator, values)) = inputs.split_first() else {
        return exec_err!("CONCAT_WS requires a separator");
    };
    if matches!(separator, Input::Null) {
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
    }
    // Reserve exact bytes for non-null inputs; masked rows can hide large unused payloads.
    let capacity = separator.capacity(rows).and_then(|separator| {
        values.iter().try_fold(
            separator.saturating_mul(values.len().saturating_sub(1)),
            |sum, value| Some(sum.saturating_add(value.capacity(rows)?)),
        )
    });
    if capacity.is_some_and(|bytes| bytes > i32::MAX as usize) {
        return exec_err!("CONCAT_WS output exceeds the Binary offset limit");
    }
    let mut bytes = Vec::with_capacity(capacity.unwrap_or(0));
    let mut offsets = Vec::with_capacity(rows + 1);
    let mut nulls = NullBufferBuilder::new(rows);
    offsets.push(0i32);
    for row in 0..rows {
        if let Some(separator) = separator.value(row) {
            let mut first = true;
            for value in values {
                if let Some(value) = value.value(row) {
                    if !first {
                        append(&mut bytes, separator)?;
                    }
                    append(&mut bytes, value)?;
                    first = false;
                }
            }
            nulls.append_non_null();
        } else {
            nulls.append_null();
        }
        offsets.push(bytes.len() as i32);
    }
    if scalar {
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))));
    }
    Ok(ColumnarValue::Array(Arc::new(BinaryArray::new(
        OffsetBuffer::new(offsets.into()),
        bytes.into(),
        nulls.finish(),
    ))))
}
