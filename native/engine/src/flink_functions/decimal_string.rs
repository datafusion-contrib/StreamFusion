use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(crate) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(DecimalString {
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalString {
    signature: Signature,
}

impl ScalarUDFImpl for DecimalString {
    fn name(&self) -> &str {
        "flink_decimal_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [DataType::Decimal128(_, 0..=38)] => Ok(DataType::Utf8),
            _ => exec_err!("Decimal formatting expects a Flink DECIMAL"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.as_slice() {
            [ColumnarValue::Array(input)] => Ok(ColumnarValue::Array(format(input)?)),
            [ColumnarValue::Scalar(input)] => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&format(&input.to_array()?)?, 0)?,
            )),
            _ => exec_err!("Decimal formatting expects one argument"),
        }
    }
}

fn format(input: &ArrayRef) -> Result<ArrayRef> {
    let input = input.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let DataType::Decimal128(precision, scale) = input.data_type() else {
        unreachable!()
    };
    let scale = *scale as usize;
    let mut offsets = Vec::with_capacity(input.len() + 1);
    offsets.push(0_i32);
    let mut bytes = Vec::with_capacity(
        input
            .len()
            .saturating_mul(usize::from(*precision).max(scale + 1) + 2),
    );
    let mut digits = itoa::Buffer::new();
    let mut text = [b'0'; 80];
    for value in input {
        let Some(value) = value else {
            offsets.push(*offsets.last().unwrap());
            continue;
        };
        let coefficient = match u64::try_from(value.unsigned_abs()) {
            Ok(value) => digits.format(value),
            Err(_) => digits.format(value.unsigned_abs()),
        };
        let sign = usize::from(value < 0);
        text[0] = b'-';
        let end = if scale == 0 {
            text[sign..sign + coefficient.len()].copy_from_slice(coefficient.as_bytes());
            sign + coefficient.len()
        } else if coefficient.len() > scale {
            let split = coefficient.len() - scale;
            text[sign..sign + split].copy_from_slice(&coefficient.as_bytes()[..split]);
            text[sign + split] = b'.';
            text[sign + split + 1..sign + coefficient.len() + 1]
                .copy_from_slice(&coefficient.as_bytes()[split..]);
            sign + coefficient.len() + 1
        } else {
            text[sign] = b'0';
            text[sign + 1] = b'.';
            let end = sign + scale + 2;
            let start = end - coefficient.len();
            text[sign + 2..start].fill(b'0');
            text[start..end].copy_from_slice(coefficient.as_bytes());
            end
        };
        bytes.extend_from_slice(&text[..end]);
        let Ok(offset) = i32::try_from(bytes.len()) else {
            return exec_err!("Decimal string batch exceeds Arrow UTF-8 offset capacity");
        };
        offsets.push(offset);
    }
    Ok(Arc::new(StringArray::new(
        OffsetBuffer::new(offsets.into()),
        bytes.into(),
        input.nulls().cloned(),
    )))
}
