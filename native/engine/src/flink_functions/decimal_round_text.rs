use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, StringBuilder};
use arrow::datatypes::{DataType, Decimal128Type, DecimalType};

/// A final STRING consumer can retain each value's runtime scale without constructing
/// Java DecimalData. Exceptional scales keep the existing whole-batch JVM evaluation.
pub(crate) fn evaluate(arrays: &[ArrayRef], truncate: bool) -> Option<ArrayRef> {
    let [values, positions] = arrays else {
        return None;
    };
    let values = values.as_any().downcast_ref::<Decimal128Array>()?;
    let positions = positions.as_any().downcast_ref::<Int32Array>()?;
    let DataType::Decimal128(precision, scale) = values.data_type() else {
        return None;
    };
    if values.len() != positions.len() || *scale < 0 || *scale > *precision as i8 {
        return None;
    }
    if values
        .iter()
        .zip(positions)
        .any(|(value, position)| value.is_some() && position.is_some_and(|position| position < -38))
    {
        return None;
    }
    let mut output = StringBuilder::with_capacity(values.len(), values.len() * 16);
    for (value, position) in values.iter().zip(positions) {
        let result = value
            .zip(position)
            .and_then(|(value, position)| round(value, *precision, *scale, position, truncate));
        match result {
            Some((value, precision, scale)) => {
                output.append_value(Decimal128Type::format_decimal(value, precision, scale));
            }
            None => output.append_null(),
        }
    }
    Some(Arc::new(output.finish()))
}

fn round(
    value: i128,
    precision: u8,
    scale: i8,
    position: i32,
    truncate: bool,
) -> Option<(i128, u8, i8)> {
    if position >= scale as i32 {
        return Some((value, precision, scale));
    }
    let output_scale = position.max(0) as i8;
    let output_precision = if position < 0 {
        (1 + precision - scale as u8).min(38)
    } else {
        1 + precision - scale as u8 + position as u8
    };
    let rounded = match 10_i128.checked_pow((scale as i32 - position) as u32) {
        None => 0,
        Some(divisor) => {
            let quotient = value / divisor;
            quotient
                + if !truncate && (value % divisor).abs() >= divisor / 2 {
                    value.signum()
                } else {
                    0
                }
        }
    };
    let unscaled = if rounded == 0 {
        0
    } else {
        rounded.checked_mul(10_i128.checked_pow((output_scale as i32 - position) as u32)?)?
    };
    let limit = 10_i128.pow(output_precision as u32);
    (unscaled > -limit && unscaled < limit).then_some((unscaled, output_precision, output_scale))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn input(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    #[test]
    fn runtime_scale_preserves_ties_sign_padding_nulls_and_slices() {
        let values = input(
            vec![
                Some(1),
                Some(123455),
                Some(-123455),
                Some(5),
                Some(0),
                None,
                Some(7),
            ],
            20,
            3,
        );
        let positions: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(2),
            Some(2),
            Some(2),
            Some(9),
            Some(i32::MIN),
            None,
        ]));
        let arrays = [values.slice(1, 6), positions.slice(1, 6)];
        let rounded = evaluate(&arrays, false).unwrap();
        assert_eq!(
            rounded.as_ref(),
            &StringArray::from(vec![
                Some("123.46"),
                Some("-123.46"),
                Some("0.01"),
                Some("0.000"),
                None,
                None
            ])
        );
        let truncated = evaluate(&arrays, true).unwrap();
        assert_eq!(
            truncated.as_ref(),
            &StringArray::from(vec![
                Some("123.45"),
                Some("-123.45"),
                Some("0.00"),
                Some("0.000"),
                None,
                None
            ])
        );
    }

    #[test]
    fn precision_overflow_and_large_negative_positions_follow_decimal_metadata() {
        let max = 10_i128.pow(38) - 1;
        assert_eq!(round(max, 38, 0, -1, false), None);
        assert_eq!(round(-max, 38, 0, -1, false), None);
        assert_eq!(round(max, 38, 0, -1, true), Some((max - 9, 38, 0)));
        assert_eq!(round(max, 38, 38, -38, false), Some((0, 1, 0)));
        assert_eq!(round(99999, 5, 2, -3, false), Some((1000, 4, 0)));
        assert_eq!(round(-99999, 5, 2, -3, true), Some((0, 4, 0)));
    }

    #[test]
    fn exceptional_nonnull_positions_leave_the_entire_batch_to_flink() {
        let arrays = [
            input(vec![Some(1234), Some(0)], 20, 3),
            Arc::new(Int32Array::from(vec![2, i32::MIN])) as ArrayRef,
        ];
        assert!(evaluate(&arrays, false).is_none());
        assert!(evaluate(&arrays, true).is_none());
    }
}
