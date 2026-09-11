//! `java.math.BigDecimal` semantics shared by the engine's decimal arithmetic and the text formats'
//! decimal parsing.

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
    let diff = target_scale as i64 - scale;
    let rescaled = if diff >= 0 {
        unscaled * BigInt::from(10u8).pow(diff as u32)
    } else {
        let divisor = BigInt::from(10u8).pow((-diff) as u32);
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
