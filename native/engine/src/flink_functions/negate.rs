use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(FlinkNegate {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkNegate {
    signature: Signature,
}

impl ScalarUDFImpl for FlinkNegate {
    fn name(&self) -> &str {
        "flink_exact_negate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(types)?;
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [t @ (DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _))] => Ok(t.clone()),
            _ => exec_err!("Exact negation requires one signed integer or DECIMAL operand"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke(&args.args)
    }
}

fn invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [input] = args else {
        return exec_err!("Exact negation requires one operand");
    };
    Ok(match input {
        ColumnarValue::Array(input) => ColumnarValue::Array(negate(input)?),
        ColumnarValue::Scalar(input) => ColumnarValue::Scalar(match input {
            ScalarValue::Int8(v) => ScalarValue::Int8(v.map(i8::wrapping_neg)),
            ScalarValue::Int16(v) => ScalarValue::Int16(v.map(i16::wrapping_neg)),
            ScalarValue::Int32(v) => ScalarValue::Int32(v.map(i32::wrapping_neg)),
            ScalarValue::Int64(v) => ScalarValue::Int64(v.map(i64::wrapping_neg)),
            ScalarValue::Decimal128(v, p, s) => {
                ScalarValue::Decimal128(v.map(i128::wrapping_neg), *p, *s)
            }
            other => return exec_err!("Unsupported exact negation input {other:?}"),
        }),
    })
}

fn negate(input: &ArrayRef) -> Result<ArrayRef> {
    Ok(arrow::compute::kernels::numeric::neg_wrapping(
        input.as_ref(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array};
    use std::sync::Arc;

    #[test]
    fn decimal_precision_scale_scalars_and_sliced_batches_are_preserved() {
        for precision in 1..=38 {
            let max = 10i128.pow(u32::from(precision)) - 1;
            for scale in 0..=precision as i8 {
                let ty = DataType::Decimal128(precision, scale);
                let input = Decimal128Array::from(vec![
                    Some(1),
                    Some(-max),
                    None,
                    Some(-1),
                    Some(0),
                    Some(max),
                    Some(1),
                ])
                .with_data_type(ty.clone())
                .slice(1, 5);
                let output = negate(&(Arc::new(input.clone()) as ArrayRef)).unwrap();
                let output = output.as_any().downcast_ref::<Decimal128Array>().unwrap();
                assert_eq!(output.data_type(), &ty);
                assert_eq!(output.nulls(), input.nulls());
                assert_eq!(
                    output.iter().collect::<Vec<_>>(),
                    vec![Some(max), None, Some(1), Some(0), Some(-max)]
                );
                assert_eq!(input.value(0), -max);
                for values in [
                    input.slice(1, 0),
                    Decimal128Array::from(vec![None; 3]).with_data_type(ty.clone()),
                ] {
                    let output = negate(&(Arc::new(values.clone()) as ArrayRef)).unwrap();
                    assert_eq!(output.data_type(), &ty);
                    assert_eq!(output.len(), values.len());
                    assert_eq!(output.null_count(), values.null_count());
                }
                for value in [Some(-max), Some(max), Some(0), None] {
                    let output = invoke(&[ColumnarValue::Scalar(ScalarValue::Decimal128(
                        value, precision, scale,
                    ))])
                    .unwrap();
                    let ColumnarValue::Scalar(output) = output else {
                        panic!("Scalar decimal negation returned an array")
                    };
                    assert_eq!(
                        output,
                        ScalarValue::Decimal128(value.map(|v| -v), precision, scale)
                    );
                }
            }
        }
    }

    #[test]
    fn widths_minima_nulls_and_sliced_validity() {
        macro_rules! check {
            ($array:ty, $native:ty) => {{
                let values = vec![
                    Some(42),
                    Some(<$native>::MIN),
                    Some(<$native>::MIN + 1),
                    None,
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(<$native>::MAX),
                    Some(-42),
                ];
                let input = <$array>::from(values).slice(1, 7);
                let output = negate(&(Arc::new(input.clone()) as ArrayRef)).unwrap();
                let output = output.as_any().downcast_ref::<$array>().unwrap();
                assert_eq!(output.data_type(), input.data_type());
                assert_eq!(output.nulls(), input.nulls());
                assert_eq!(
                    output.iter().collect::<Vec<_>>(),
                    vec![
                        Some(<$native>::MIN),
                        Some(<$native>::MAX),
                        None,
                        Some(1),
                        Some(0),
                        Some(-1),
                        Some(-<$native>::MAX),
                    ]
                );
                assert_eq!(input.value(0), <$native>::MIN);
                for empty in [input.slice(3, 0), <$array>::from(vec![None; 3])] {
                    let result = negate(&(Arc::new(empty.clone()) as ArrayRef)).unwrap();
                    assert_eq!(result.len(), empty.len());
                    assert_eq!(result.null_count(), empty.null_count());
                }
            }};
        }
        check!(Int8Array, i8);
        check!(Int16Array, i16);
        check!(Int32Array, i32);
        check!(Int64Array, i64);
    }

    #[test]
    fn scalar_minima_and_nulls_wrap_instead_of_using_datafusions_checked_negation() {
        for scalar in [
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int8(None),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int16(None),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int32(None),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(None),
        ] {
            let args = [ColumnarValue::Scalar(scalar.clone())];
            let output = invoke(&args).unwrap();
            let ColumnarValue::Scalar(output) = output else {
                panic!("Scalar negation returned an array");
            };
            assert_eq!(output, scalar);
        }
    }

    #[test]
    fn compiled_calc_materializes_wrapped_literal_minima_without_a_jvm() {
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        for (kind, scalar) in [
            (9, ScalarValue::Int8(Some(i8::MIN))),
            (8, ScalarValue::Int16(Some(i16::MIN))),
            (7, ScalarValue::Int32(Some(i32::MIN))),
            (1, ScalarValue::Int64(Some(i64::MIN))),
        ] {
            let mut program = crate::bench::CalcProgram::new(
                vec![6, kind],
                vec![219, 0],
                vec![1, 0],
                vec![scalar.to_string().parse().unwrap()],
                vec![],
                vec![],
            );
            let output = program.run(batch.clone());
            assert_eq!(output.column(0), &scalar.to_array_of_size(3).unwrap());
        }
    }

    #[test]
    fn rejects_unsupported_widths_and_arities() {
        let f = function();
        for types in [
            vec![],
            vec![DataType::Float64],
            vec![DataType::Decimal256(38, 3)],
            vec![DataType::Int32, DataType::Int64],
        ] {
            assert!(f.return_type(&types).is_err());
        }
        assert!(invoke(&[]).is_err());
    }
}
