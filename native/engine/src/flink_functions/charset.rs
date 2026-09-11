use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Clone, Copy)]
pub(super) enum Charset {
    Utf8,
    Ascii,
    Latin1,
    Utf16,
    Utf16Be,
    Utf16Le,
}

pub(super) fn function(decode: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(CharsetFunction {
        decode,
        signature: Signature::exact(
            vec![
                if decode {
                    DataType::Binary
                } else {
                    DataType::Utf8
                },
                DataType::Utf8,
            ],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CharsetFunction {
    decode: bool,
    signature: Signature,
}

impl ScalarUDFImpl for CharsetFunction {
    fn name(&self) -> &str {
        if self.decode {
            "flink_decode"
        } else {
            "flink_encode"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(if self.decode {
            DataType::Utf8
        } else {
            DataType::Binary
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input, ColumnarValue::Scalar(ScalarValue::Utf8(Some(charset)))] = args.args.as_slice()
        else {
            return exec_err!("ENCODE/DECODE expects an input and a non-null literal charset");
        };
        let charset = match charset.as_str() {
            "UTF-8" => Charset::Utf8,
            "US-ASCII" => Charset::Ascii,
            "ISO-8859-1" => Charset::Latin1,
            "UTF-16" => Charset::Utf16,
            "UTF-16BE" => Charset::Utf16Be,
            "UTF-16LE" => Charset::Utf16Le,
            other => return exec_err!("Unsupported ENCODE/DECODE charset: {other}"),
        };
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input: ArrayRef = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let output = if self.decode {
            super::decode::decode(&input, charset)?
        } else {
            super::encode::encode(&input, charset)?
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}
