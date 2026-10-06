//! `java.math.BigDecimal` semantics shared by the engine's decimal arithmetic and the text formats'
//! decimal parsing.

use arrow::datatypes::i256;
use arrow::error::ArrowError;

/// A prepared HALF_UP rescale with Flink's declared precision bound. The column ABI stays
/// Decimal128 even when an arithmetic intermediate requires 256 bits.
pub struct DecimalRescale {
    increase: bool,
    factor: i256,
    narrow_factor: Option<i128>,
    limit: i128,
}

impl DecimalRescale {
    pub fn new(source_scale: i16, precision: u8, scale: i8) -> Result<Self, ArrowError> {
        if !(1..=38).contains(&precision)
            || !(0..=precision as i8).contains(&scale)
            || !(0..=76).contains(&source_scale)
        {
            return Err(ArrowError::InvalidArgumentError(format!(
                "invalid Flink decimal rescale: scale {source_scale} to ({precision}, {scale})"
            )));
        }
        let difference = scale as i16 - source_scale;
        let factor = i256::from_i128(10).wrapping_pow(difference.unsigned_abs() as u32);
        Ok(Self {
            increase: difference >= 0,
            factor,
            narrow_factor: factor.to_i128(),
            limit: 10_i128.pow(precision as u32),
        })
    }

    pub fn narrow(&self, value: i128) -> Option<i128> {
        let rounded = if self.increase {
            value.checked_mul(self.narrow_factor?)?
        } else if let Some(divisor) = self.narrow_factor {
            let quotient = value / divisor;
            let remainder = value % divisor;
            quotient
                + if remainder.abs() >= divisor / 2 {
                    value.signum()
                } else {
                    0
                }
        } else {
            // A divisor of at least 10^39 rounds every i128 value to zero.
            0
        };
        self.check_precision(rounded)
    }

    pub fn wide(&self, value: i256) -> Option<i128> {
        let rounded = if self.increase {
            value.checked_mul(self.factor)?
        } else {
            let quotient = value / self.factor;
            let remainder = value % self.factor;
            let negative = value < i256::ZERO;
            let magnitude = if negative { -remainder } else { remainder };
            if magnitude >= self.factor / i256::from_i128(2) {
                if negative {
                    quotient - i256::ONE
                } else {
                    quotient + i256::ONE
                }
            } else {
                quotient
            }
        };
        self.check_precision(rounded.to_i128()?)
    }

    fn check_precision(&self, value: i128) -> Option<i128> {
        (value > -self.limit && value < self.limit).then_some(value)
    }
}

/// `BigDecimal.setScale(scale, HALF_UP)` + `DecimalData.fromBigDecimal`'s precision check: rescale an
/// (unscaled, scale) big value to `target_scale`, rounding half away from zero, and return None (SQL
/// NULL) when the result needs more than `precision` digits.
pub fn rescale_half_up(
    unscaled: num_bigint::BigInt,
    scale: i64,
    precision: u8,
    target_scale: i8,
) -> Option<i128> {
    use num_bigint::BigInt;
    if unscaled.sign() == num_bigint::Sign::NoSign {
        return Some(0);
    }
    // Bound any power of ten by the input's digits or the target precision, rather than an
    // arbitrary exponent from the message. Entirely discarded values round without a divisor.
    let digits = unscaled.magnitude().to_string();
    let width = digits.len() as i128;
    let diff = i128::from(target_scale) - i128::from(scale);
    let rescaled = if diff >= 0 {
        if width + diff > i128::from(precision) {
            return None;
        }
        unscaled * BigInt::from(10u8).pow(u32::try_from(diff).ok()?)
    } else {
        let dropped = -diff;
        if dropped > width {
            return Some(0);
        }
        if dropped == width {
            if digits.as_bytes()[0] >= b'5' && precision == 0 {
                return None;
            }
            return Some(if digits.as_bytes()[0] >= b'5' {
                if unscaled.sign() == num_bigint::Sign::Minus {
                    -1
                } else {
                    1
                }
            } else {
                0
            });
        }
        if width - dropped > i128::from(precision) {
            return None;
        }
        let divisor = BigInt::from(10u8).pow(u32::try_from(dropped).ok()?);
        // BigInt `/`/`%` truncate toward zero, so q/r carry the value's sign.
        let q = &unscaled / &divisor;
        let r = &unscaled % &divisor;
        // HALF_UP: round away from zero when the dropped fraction is at least half.
        if r.magnitude() * 2u8 >= *divisor.magnitude() {
            if unscaled.sign() == num_bigint::Sign::Minus {
                q - 1
            } else {
                q + 1
            }
        } else {
            q
        }
    };
    // BigDecimal.precision(): digits of the unscaled value (zero has precision 1 — always fits).
    if rescaled.magnitude().to_string().len() > precision as usize
        && rescaled.sign() != num_bigint::Sign::NoSign
    {
        return None;
    }
    i128::try_from(&rescaled).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    #[test]
    fn extreme_scales_short_circuit_zero_overflow_and_underflow() {
        for scale in [i64::MIN, -2_147_483_647, 2_147_483_647, i64::MAX] {
            assert_eq!(rescale_half_up(BigInt::from(0), scale, 5, 2), Some(0));
        }
        assert_eq!(rescale_half_up(BigInt::from(1), -100_000_000, 5, 2), None);
        assert_eq!(rescale_half_up(BigInt::from(1), 100_000_000, 5, 2), Some(0));
        assert_eq!(rescale_half_up(BigInt::from(1), i64::MIN, 5, 2), None);
        assert_eq!(rescale_half_up(BigInt::from(1), i64::MAX, 5, 2), Some(0));
        assert_eq!(rescale_half_up(BigInt::from(499), 3, 5, 0), Some(0));
        assert_eq!(rescale_half_up(BigInt::from(500), 3, 5, 0), Some(1));
        assert_eq!(rescale_half_up(BigInt::from(-500), 3, 5, 0), Some(-1));
        assert_eq!(rescale_half_up(BigInt::from(999), 2, 1, 0), None);
    }

    #[test]
    fn fixed_width_rescale_matches_big_integer_rounding_and_overflow() {
        let values = [
            i128::MIN,
            -10_i128.pow(38) + 1,
            -999995,
            -15,
            -5,
            -1,
            0,
            1,
            5,
            15,
            999995,
            10_i128.pow(38) - 1,
            i128::MAX,
        ];
        for precision in [1, 9, 18, 19, 28, 38] {
            for scale in 0..=precision as i8 {
                for source_scale in 0..=76 {
                    let rescale = DecimalRescale::new(source_scale, precision, scale).unwrap();
                    for value in values {
                        let expected = rescale_half_up(
                            BigInt::from(value),
                            source_scale as i64,
                            precision,
                            scale,
                        );
                        assert_eq!(
                            rescale.narrow(value),
                            expected,
                            "{value}, scale {source_scale} -> ({precision}, {scale})"
                        );
                        assert_eq!(rescale.wide(i256::from_i128(value)), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn wide_rescale_keeps_values_whose_intermediate_exceeds_decimal128() {
        let max = 10_i128.pow(38) - 1;
        for left in [max, -max, max / 2] {
            for right in [max, -max, 15] {
                let wide = i256::from_i128(left) * i256::from_i128(right);
                for source_scale in [0, 19, 38, 57, 76] {
                    for scale in [0, 6, 19, 38] {
                        let expected = rescale_half_up(
                            BigInt::from(left) * BigInt::from(right),
                            source_scale as i64,
                            38,
                            scale,
                        );
                        assert_eq!(
                            DecimalRescale::new(source_scale, 38, scale)
                                .unwrap()
                                .wide(wide),
                            expected
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn rescale_rejects_non_flink_types() {
        for (source, precision, scale) in [
            (0, 0, 0),
            (0, 39, 0),
            (0, 3, 4),
            (0, 3, -1),
            (-1, 3, 0),
            (77, 3, 0),
        ] {
            assert!(DecimalRescale::new(source, precision, scale).is_err());
        }
    }
}
