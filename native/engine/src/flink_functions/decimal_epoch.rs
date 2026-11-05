use arrow::array::Array;
use arrow::datatypes::{DataType, Decimal128Type, Int64Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(seconds: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(DecimalEpoch {
        seconds,
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalEpoch {
    seconds: bool,
    signature: Signature,
}

impl ScalarUDFImpl for DecimalEpoch {
    fn name(&self) -> &str {
        if self.seconds {
            "flink_decimal_epoch_seconds"
        } else {
            "flink_decimal_epoch_millis"
        }
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
            [DataType::Decimal128(p, s)] if (1..=38).contains(p) && (0..=*p as i8).contains(s) => {
                Ok(streamfusion_bridge::timestamp::timestamp_type())
            }
            _ => exec_err!("Decimal epoch requires one Flink DECIMAL operand"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        let input = args.args[0].clone().into_array(args.number_rows)?;
        if input.len() != args.number_rows {
            return exec_err!("Decimal epoch array length differs from batch length");
        }
        let input = as_primitive_array::<Decimal128Type>(&input)?;
        let DataType::Decimal128(precision, scale) = input.data_type() else {
            unreachable!()
        };
        let units = |value: i64| {
            if self.seconds {
                value.wrapping_mul(1000)
            } else {
                value
            }
        };
        // Released Flink truncates toward zero, wraps BigDecimal.longValue(), then scales
        // seconds with wrapping multiplication. Only the resulting milliseconds are bounded.
        let millis = if *scale == 0 {
            input.unary::<_, Int64Type>(|value| units(value as i64))
        } else if *precision <= 18 {
            let divisor = 10_i64.pow(*scale as u32);
            input.unary::<_, Int64Type>(|value| units(value as i64 / divisor))
        } else {
            let divisor = 10_i128.pow(*scale as u32);
            input.unary::<_, Int64Type>(|value| units((value / divisor) as i64))
        };
        Ok(ColumnarValue::Array(
            super::unnest_temporal::epoch_timestamp::<false>(&millis),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Decimal128Array};
    use datafusion::common::{config::ConfigOptions, ScalarValue};
    use num_bigint::BigInt;
    use num_traits::ToPrimitive;
    use std::sync::Arc;
    use streamfusion_bridge::timestamp::{timestamp_type, TimestampColumn};

    fn invoke(input: ColumnarValue, seconds: bool, rows: usize) -> ArrayRef {
        function(seconds)
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![input],
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(arrow::datatypes::Field::new(
                    "result",
                    timestamp_type(),
                    true,
                )),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .into_array(rows)
            .unwrap()
    }

    fn oracle(coefficient: i128, scale: i8, seconds: bool) -> Option<i64> {
        let units = BigInt::from(coefficient) / BigInt::from(10).pow(scale as u32);
        let modulus: BigInt = BigInt::from(1) << 64;
        let mut signed: BigInt = units % &modulus;
        if signed < BigInt::from(0) {
            signed += &modulus;
        }
        if signed >= BigInt::from(1) << 63 {
            signed -= &modulus;
        }
        if seconds {
            signed = (signed * 1000) % &modulus;
            if signed < BigInt::from(0) {
                signed += &modulus;
            }
            if signed >= BigInt::from(1) << 63 {
                signed -= &modulus;
            }
        }
        let millis = signed.to_i64().unwrap();
        (-62_167_219_200_000..=253_402_300_799_999)
            .contains(&millis)
            .then_some(millis)
    }

    #[test]
    fn every_precision_and_scale_matches_wide_integer_oracle() {
        let mut random = 20261105_u128;
        for precision in 1..=38 {
            let maximum = 10_i128.pow(precision as u32) - 1;
            for scale in 0..=precision as i8 {
                let mut values = vec![
                    Some(maximum),
                    Some(-maximum),
                    None,
                    Some(-1),
                    Some(0),
                    Some(1),
                ];
                for _ in 0..64 {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    let value = (random % (maximum as u128 + 1)) as i128;
                    values.push(Some(if random & 1 == 0 { value } else { -value }));
                }
                let input = Decimal128Array::from(values.clone())
                    .with_precision_and_scale(precision, scale)
                    .unwrap();
                for seconds in [false, true] {
                    let output = invoke(
                        ColumnarValue::Array(Arc::new(input.clone())),
                        seconds,
                        input.len(),
                    );
                    output.to_data().validate_full().unwrap();
                    let column = TimestampColumn::try_new(output.as_ref()).unwrap();
                    for (row, value) in values.iter().enumerate() {
                        let expected = value.and_then(|v| oracle(v, scale, seconds));
                        assert_eq!(
                            !column.is_null(row),
                            expected.is_some(),
                            "({precision},{scale}) {value:?} seconds={seconds}"
                        );
                        if let Some(expected) = expected {
                            assert_eq!(column.value(row).unwrap().millis(), expected);
                            assert_eq!(column.value(row).unwrap().nano_of_milli(), 0);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn truncation_wrap_boundaries_and_slices_match_released_decimal_rules() {
        let integers = [
            i64::MIN as i128,
            i64::MAX as i128,
            1_i128 << 64,
            (1_i128 << 64) + 1,
            -(1_i128 << 64) - 1,
            -62_167_219_200_001,
            -62_167_219_200_000,
            -62_167_219_200,
            253_402_300_799,
            253_402_300_799_999,
            253_402_300_800_000,
        ];
        for scale in [0, 3, 18] {
            let factor = 10_i128.pow(scale as u32);
            let mut values = vec![None, Some(1)];
            for integer in integers {
                for delta in [-1, 0, 1] {
                    let value = integer * factor + delta;
                    if value.abs() < 10_i128.pow(38) {
                        values.push(Some(value));
                    }
                }
            }
            values.push(None);
            let input = Decimal128Array::from(values)
                .with_precision_and_scale(38, scale)
                .unwrap();
            let input = input.slice(1, input.len() - 2);
            for seconds in [false, true] {
                let output = invoke(
                    ColumnarValue::Array(Arc::new(input.clone())),
                    seconds,
                    input.len(),
                );
                let result = TimestampColumn::try_new(output.as_ref()).unwrap();
                for row in 0..input.len() {
                    let expected = input
                        .is_valid(row)
                        .then(|| input.value(row))
                        .and_then(|v| oracle(v, scale, seconds));
                    assert_eq!(!result.is_null(row), expected.is_some());
                    if let Some(millis) = expected {
                        assert_eq!(result.value(row).unwrap().millis(), millis);
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_null_and_empty_inputs_preserve_timestamp_transport() {
        for seconds in [false, true] {
            for value in [None, Some(-1001), Some((1_i128 << 64) + 1)] {
                for rows in [0, 7] {
                    let output = invoke(
                        ColumnarValue::Scalar(ScalarValue::Decimal128(value, 38, 0)),
                        seconds,
                        rows,
                    );
                    assert_eq!(output.len(), rows);
                    assert_eq!(
                        output.null_count(),
                        if value.and_then(|v| oracle(v, 0, seconds)).is_none() {
                            rows
                        } else {
                            0
                        }
                    );
                    output.to_data().validate_full().unwrap();
                }
            }
        }
    }

    #[test]
    fn invalid_signatures_and_batch_lengths_are_rejected() {
        let f = function(false);
        for types in [
            vec![],
            vec![DataType::Int64],
            vec![DataType::Decimal128(38, -1)],
            vec![DataType::Decimal128(38, 39)],
            vec![DataType::Decimal128(0, 0)],
        ] {
            assert!(f.return_type(&types).is_err());
        }
        let input = Arc::new(
            Decimal128Array::from(vec![1])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        );
        assert!(f
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input)],
                arg_fields: vec![],
                number_rows: 2,
                return_field: Arc::new(arrow::datatypes::Field::new(
                    "result",
                    timestamp_type(),
                    true
                )),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .is_err());
    }
}
