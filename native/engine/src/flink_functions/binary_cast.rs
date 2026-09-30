use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(crate) fn function(length: usize, pad: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(BinaryCast {
        length,
        pad,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryCast {
    length: usize,
    pad: bool,
    signature: Signature,
}

impl ScalarUDFImpl for BinaryCast {
    fn name(&self) -> &str {
        "flink_binary_cast"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(|arrays| self.cast(arrays), vec![])(
            &args.args,
        )
    }
}

impl BinaryCast {
    fn cast(&self, arrays: &[ArrayRef]) -> Result<ArrayRef> {
        let [input] = arrays else {
            return exec_err!("Binary cast expects one argument");
        };
        let binary = arrow::compute::cast(input, &DataType::Binary)?;
        let values = binary.as_any().downcast_ref::<BinaryArray>().unwrap();
        if self.length == usize::MAX {
            return Ok(binary);
        }
        let mut output = BinaryBuilder::with_capacity(
            values.len(),
            values
                .value_data()
                .len()
                .min(values.len().saturating_mul(self.length)),
        );
        let mut padded = Vec::new();
        for value in values {
            match value {
                None => output.append_null(),
                Some(value) => {
                    let take = value.len().min(self.length);
                    if self.pad && take < self.length {
                        padded.clear();
                        padded.extend_from_slice(value);
                        padded.resize(self.length, 0);
                        output.append_value(&padded);
                    } else {
                        output.append_value(&value[..take]);
                    }
                }
            }
        }
        Ok(Arc::new(output.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, StringArray};

    #[test]
    fn casts_preserve_slices_nulls_and_partial_utf8_bytes() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("unused"),
            Some("é中"),
            Some(""),
            None,
            Some("a\0b"),
        ]));
        let input = input.slice(1, 4);
        let cast = BinaryCast {
            length: 1,
            pad: true,
            signature: Signature::any(1, Volatility::Immutable),
        };
        let output = cast.cast(&[input.clone()]).unwrap();
        assert_eq!(
            output.as_ref(),
            &BinaryArray::from(vec![
                Some(&[0xc3][..]),
                Some(&[0][..]),
                None,
                Some(&[b'a'][..])
            ])
        );
        let cast = BinaryCast { length: 4, ..cast };
        let output = cast.cast(&[input]).unwrap();
        assert_eq!(
            output.as_ref(),
            &BinaryArray::from(vec![
                Some(&[0xc3, 0xa9, 0xe4, 0xb8][..]),
                Some(&[0, 0, 0, 0][..]),
                None,
                Some(&[b'a', 0, b'b', 0][..])
            ])
        );
    }

    #[test]
    fn variable_and_legacy_casts_do_not_pad_and_fixed_inputs_keep_raw_bytes() {
        let input: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some([0xff, 0]), None, Some([0, 0x80])].into_iter(),
                2,
            )
            .unwrap(),
        );
        for length in [4, usize::MAX] {
            let cast = BinaryCast {
                length,
                pad: false,
                signature: Signature::any(1, Volatility::Immutable),
            };
            let output = cast.cast(&[input.clone()]).unwrap();
            assert_eq!(
                output.as_ref(),
                &BinaryArray::from(vec![Some(&[0xff, 0][..]), None, Some(&[0, 0x80][..])])
            );
        }
    }
}
