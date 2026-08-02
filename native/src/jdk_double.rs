//! Byte-exact port of the legacy JDK float-to-decimal formatting: `Double.toString` /
//! `Float.toString` as implemented by `jdk.internal.math.FloatingDecimal` (with its
//! `FDBigInteger` arithmetic) on JDK ≤ 18. The parity target is JDK 17. JDK 19 replaced the
//! algorithm with shortest-representation (Ryū-style) output, which differs from the legacy
//! spelling on a measured 0.3% of random doubles and 11% of random floats — so a
//! shortest-digits formatter (Rust's `Display`, the ryu crate) cannot substitute. The planner
//! probes at runtime that the host JVM still spells like this port before admitting FLOAT/DOUBLE
//! columns to the native text encoders.
//!
//! Ported mechanically from OpenJDK 17 (GPLv2 + Classpath exception): same variable structure,
//! same rounding-correction loops, Java's wrapping integer semantics kept via explicit wrapping
//! ops. Only what binary-to-ASCII conversion reaches is ported; the ASCII-to-binary direction is
//! not needed.

const EXP_SHIFT: i32 = 52;
const FRACT_HOB: u64 = 1 << EXP_SHIFT;
const EXP_ONE: u64 = (EXP_BIAS as u64) << EXP_SHIFT;
const MAX_SMALL_BIN_EXP: i32 = 62;
const MIN_SMALL_BIN_EXP: i32 = -(63 / 3);
const SIGN_BIT_MASK: u64 = 0x8000_0000_0000_0000;
const EXP_BIT_MASK: u64 = 0x7FF0_0000_0000_0000;
const SIGNIF_BIT_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
const EXP_BIAS: i32 = 1023;

const SINGLE_EXP_SHIFT: i32 = 23;
const SINGLE_FRACT_HOB: u32 = 1 << SINGLE_EXP_SHIFT;
const SINGLE_SIGN_BIT_MASK: u32 = 0x8000_0000;
const SINGLE_EXP_BIT_MASK: u32 = 0x7F80_0000;
const SINGLE_SIGNIF_BIT_MASK: u32 = 0x007F_FFFF;
const SINGLE_EXP_BIAS: i32 = 127;

// Like the format codes, this helper stays ungated (a featureless core build carries it unused):
// the consumers are the feature-gated text encoders and the JVM's spelling probe.
#[allow(dead_code)]
pub(crate) fn jdk_double_to_string(value: f64, out: &mut Vec<u8>) {
    let d_bits = value.to_bits();
    let is_negative = (d_bits & SIGN_BIT_MASK) != 0;
    let mut fract_bits = d_bits & SIGNIF_BIT_MASK;
    let mut bin_exp = ((d_bits & EXP_BIT_MASK) >> EXP_SHIFT) as i32;
    if bin_exp == (EXP_BIT_MASK >> EXP_SHIFT) as i32 {
        out.extend_from_slice(exceptional(fract_bits == 0, is_negative));
        return;
    }
    let n_significant_bits;
    if bin_exp == 0 {
        if fract_bits == 0 {
            out.extend_from_slice(if is_negative { b"-0.0" } else { b"0.0" });
            return;
        }
        let leading_zeros = fract_bits.leading_zeros() as i32;
        let shift = leading_zeros - (63 - EXP_SHIFT);
        fract_bits <<= shift;
        bin_exp = 1 - shift;
        n_significant_bits = 64 - leading_zeros;
    } else {
        fract_bits |= FRACT_HOB;
        n_significant_bits = EXP_SHIFT + 1;
    }
    bin_exp -= EXP_BIAS;
    let mut buffer = BinaryToAsciiBuffer::new(is_negative);
    buffer.dtoa(bin_exp, fract_bits, n_significant_bits);
    buffer.get_chars(out);
}

#[allow(dead_code)]
pub(crate) fn jdk_float_to_string(value: f32, out: &mut Vec<u8>) {
    let f_bits = value.to_bits();
    let is_negative = (f_bits & SINGLE_SIGN_BIT_MASK) != 0;
    let mut fract_bits = f_bits & SINGLE_SIGNIF_BIT_MASK;
    let mut bin_exp = ((f_bits & SINGLE_EXP_BIT_MASK) >> SINGLE_EXP_SHIFT) as i32;
    if bin_exp == (SINGLE_EXP_BIT_MASK >> SINGLE_EXP_SHIFT) as i32 {
        out.extend_from_slice(exceptional(fract_bits == 0, is_negative));
        return;
    }
    let n_significant_bits;
    if bin_exp == 0 {
        if fract_bits == 0 {
            out.extend_from_slice(if is_negative { b"-0.0" } else { b"0.0" });
            return;
        }
        let leading_zeros = fract_bits.leading_zeros() as i32;
        let shift = leading_zeros - (31 - SINGLE_EXP_SHIFT);
        fract_bits <<= shift;
        bin_exp = 1 - shift;
        n_significant_bits = 32 - leading_zeros;
    } else {
        fract_bits |= SINGLE_FRACT_HOB;
        n_significant_bits = SINGLE_EXP_SHIFT + 1;
    }
    bin_exp -= SINGLE_EXP_BIAS;
    let mut buffer = BinaryToAsciiBuffer::new(is_negative);
    buffer.dtoa(
        bin_exp,
        u64::from(fract_bits) << (EXP_SHIFT - SINGLE_EXP_SHIFT),
        n_significant_bits,
    );
    buffer.get_chars(out);
}

fn exceptional(is_infinite: bool, is_negative: bool) -> &'static [u8] {
    match (is_infinite, is_negative) {
        (true, false) => b"Infinity",
        (true, true) => b"-Infinity",
        (false, _) => b"NaN",
    }
}

struct BinaryToAsciiBuffer {
    is_negative: bool,
    dec_exponent: i32,
    first_digit_index: usize,
    n_digits: usize,
    digits: [u8; 20],
}

impl BinaryToAsciiBuffer {
    fn new(is_negative: bool) -> BinaryToAsciiBuffer {
        BinaryToAsciiBuffer {
            is_negative,
            dec_exponent: 0,
            first_digit_index: 0,
            n_digits: 0,
            digits: [0; 20],
        }
    }

    /// The easy subcase: all significant bits, after scaling, are held in `lvalue` (a positive
    /// finite number). Java splits this into int and long loops purely as a fast path; both
    /// develop identical digits, so one u64 loop suffices.
    fn develop_long_digits(
        &mut self,
        mut dec_exponent: i32,
        mut lvalue: u64,
        insignificant_digits: i32,
    ) {
        if insignificant_digits != 0 {
            // Discard non-significant low-order digits while rounding to the insignificant value.
            let pow10 = LONG_5_POW[insignificant_digits as usize] << insignificant_digits;
            let residue = lvalue % pow10;
            lvalue /= pow10;
            dec_exponent += insignificant_digits;
            if residue >= (pow10 >> 1) {
                lvalue += 1;
            }
        }
        let mut digitno = self.digits.len() - 1;
        let mut c = (lvalue % 10) as u8;
        lvalue /= 10;
        while c == 0 {
            dec_exponent += 1;
            c = (lvalue % 10) as u8;
            lvalue /= 10;
        }
        while lvalue != 0 {
            self.digits[digitno] = c + b'0';
            digitno -= 1;
            dec_exponent += 1;
            c = (lvalue % 10) as u8;
            lvalue /= 10;
        }
        self.digits[digitno] = c + b'0';
        self.dec_exponent = dec_exponent + 1;
        self.first_digit_index = digitno;
        self.n_digits = self.digits.len() - digitno;
    }

    /// `FloatingDecimal.BinaryToASCIIBuffer.dtoa` with `isCompatibleFormat` fixed to true — the
    /// only value `toJavaFormatString` ever passes.
    fn dtoa(&mut self, bin_exp: i32, mut fract_bits: u64, n_significant_bits: i32) {
        debug_assert!(fract_bits & FRACT_HOB != 0);
        let tail_zeros = fract_bits.trailing_zeros() as i32;
        let n_fract_bits = EXP_SHIFT + 1 - tail_zeros;
        // Number of significant bits to the right of the point.
        let n_tiny_bits = (n_fract_bits - bin_exp - 1).max(0);
        if (MIN_SMALL_BIN_EXP..=MAX_SMALL_BIN_EXP).contains(&bin_exp)
            && (n_tiny_bits as usize) < LONG_5_POW.len()
            && n_fract_bits + N_5_BITS[n_tiny_bits as usize] < 64
            && n_tiny_bits == 0
        {
            let insignificant = if bin_exp > n_significant_bits {
                insignificant_digits_for_pow2(bin_exp - n_significant_bits - 1)
            } else {
                0
            };
            if bin_exp >= EXP_SHIFT {
                fract_bits <<= bin_exp - EXP_SHIFT;
            } else {
                fract_bits >>= EXP_SHIFT - bin_exp;
            }
            self.develop_long_digits(0, fract_bits, insignificant);
            return;
        }
        // The hard case: compute large positive integers B and S and integer dec_exp such that
        //   d = (B / S) * 10^dec_exp,   1 <= B / S < 10
        // plus M = half an ULP of d, scaled like B: iterate B/S picking off quotient digits and
        // stop when the remainder is <= M.
        let mut dec_exp = estimate_dec_exp(fract_bits, bin_exp);

        let b5 = (-dec_exp).max(0);
        let mut b2 = b5 + n_tiny_bits + bin_exp;
        let s5 = dec_exp.max(0);
        let mut s2 = s5 + n_tiny_bits;
        let m5 = b5;
        let mut m2 = b2 - n_significant_bits;

        fract_bits >>= tail_zeros;
        b2 -= n_fract_bits - 1;
        let common2factor = b2.min(s2);
        b2 -= common2factor;
        s2 -= common2factor;
        m2 -= common2factor;

        // For exact powers of two, the next smallest number is only half as far away (the meaning
        // of ULP changes at power-of-two bounds), so halve M.
        if n_fract_bits == 1 {
            m2 -= 1;
        }
        if m2 < 0 {
            b2 -= m2;
            s2 -= m2;
            m2 = 0;
        }

        let mut ndigit;
        let mut low;
        let mut high;
        let low_digit_difference: i64;
        let mut q: i32;

        // Where all values fit in int or long integers, avoid FDBigInteger arithmetic.
        let n5bits = |p5: i32| {
            if (p5 as usize) < N_5_BITS.len() {
                N_5_BITS[p5 as usize]
            } else {
                p5 * 3
            }
        };
        let bbits = n_fract_bits + b2 + n5bits(b5);
        let ten_sbits = s2 + 1 + n5bits(s5 + 1);
        if bbits < 64 && ten_sbits < 64 {
            if bbits < 32 && ten_sbits < 32 {
                // Java's all-int branch, with its wrapping 32-bit semantics.
                let mut b = (fract_bits as i32)
                    .wrapping_mul(SMALL_5_POW[b5 as usize] as i32)
                    .wrapping_shl(b2 as u32);
                let s = (SMALL_5_POW[s5 as usize] as i32).wrapping_shl(s2 as u32);
                let mut m = (SMALL_5_POW[m5 as usize] as i32).wrapping_shl(m2 as u32);
                let tens = s.wrapping_mul(10);
                // Unroll the first iteration: if the dec_exp estimate was too high the first
                // quotient is zero, which is discarded while dec_exp is decremented.
                ndigit = 0;
                q = b / s;
                b = 10i32.wrapping_mul(b % s);
                m = m.wrapping_mul(10);
                low = b < m;
                high = b.wrapping_add(m) > tens;
                if q == 0 && !high {
                    dec_exp -= 1;
                } else {
                    self.digits[ndigit] = b'0' + q as u8;
                    ndigit += 1;
                }
                // Java always has at least one digit after the point in either F- or E-form, so
                // E-form needs more than one digit.
                if !(-3..8).contains(&dec_exp) {
                    high = false;
                    low = false;
                }
                while !low && !high {
                    q = b / s;
                    b = 10i32.wrapping_mul(b % s);
                    m = m.wrapping_mul(10);
                    if m > 0 {
                        low = b < m;
                        high = b.wrapping_add(m) > tens;
                    } else {
                        // m overflowed: it is certainly > b, and b+m > tens too.
                        low = true;
                        high = true;
                    }
                    self.digits[ndigit] = b'0' + q as u8;
                    ndigit += 1;
                }
                low_digit_difference = i64::from(b.wrapping_shl(1).wrapping_sub(tens));
            } else {
                // Java's all-long branch, with its wrapping 64-bit semantics.
                let mut b = (fract_bits as i64)
                    .wrapping_mul(LONG_5_POW[b5 as usize] as i64)
                    .wrapping_shl(b2 as u32);
                let s = (LONG_5_POW[s5 as usize] as i64).wrapping_shl(s2 as u32);
                let mut m = (LONG_5_POW[m5 as usize] as i64).wrapping_shl(m2 as u32);
                let tens = s.wrapping_mul(10);
                ndigit = 0;
                q = (b / s) as i32;
                b = 10i64.wrapping_mul(b % s);
                m = m.wrapping_mul(10);
                low = b < m;
                high = b.wrapping_add(m) > tens;
                if q == 0 && !high {
                    dec_exp -= 1;
                } else {
                    self.digits[ndigit] = b'0' + q as u8;
                    ndigit += 1;
                }
                if !(-3..8).contains(&dec_exp) {
                    high = false;
                    low = false;
                }
                while !low && !high {
                    q = (b / s) as i32;
                    b = 10i64.wrapping_mul(b % s);
                    m = m.wrapping_mul(10);
                    if m > 0 {
                        low = b < m;
                        high = b.wrapping_add(m) > tens;
                    } else {
                        low = true;
                        high = true;
                    }
                    self.digits[ndigit] = b'0' + q as u8;
                    ndigit += 1;
                }
                low_digit_difference = b.wrapping_shl(1).wrapping_sub(tens);
            }
        } else {
            let mut sval = FdBigInt::value_of_pow_52(s5, s2);
            let shift_bias = sval.normalization_bias();
            sval.left_shift(shift_bias); // Normalize so that division works better.
            let mut bval = FdBigInt::value_of_mul_pow_52(fract_bits, b5, b2 + shift_bias);
            let mut mval = FdBigInt::value_of_pow_52(m5 + 1, m2 + shift_bias + 1);
            let ten_sval = FdBigInt::value_of_pow_52(s5 + 1, s2 + shift_bias + 1);

            ndigit = 0;
            q = bval.quo_rem_iteration(&sval);
            low = bval.cmp(&mval) < 0;
            high = ten_sval.add_and_cmp(&bval, &mval) <= 0;
            if q == 0 && !high {
                dec_exp -= 1;
            } else {
                self.digits[ndigit] = b'0' + q as u8;
                ndigit += 1;
            }
            if !(-3..8).contains(&dec_exp) {
                high = false;
                low = false;
            }
            while !low && !high {
                q = bval.quo_rem_iteration(&sval);
                mval.mult_by_10();
                low = bval.cmp(&mval) < 0;
                high = ten_sval.add_and_cmp(&bval, &mval) <= 0;
                self.digits[ndigit] = b'0' + q as u8;
                ndigit += 1;
            }
            if high && low {
                bval.left_shift(1);
                low_digit_difference = i64::from(bval.cmp(&ten_sval));
            } else {
                low_digit_difference = 0;
            }
        }
        self.dec_exponent = dec_exp + 1;
        self.first_digit_index = 0;
        self.n_digits = ndigit;
        // The last digit gets rounded based on the stopping condition.
        if high {
            if low {
                match low_digit_difference.cmp(&0) {
                    // A tie: choose based on which digits we like.
                    std::cmp::Ordering::Equal => {
                        if self.digits[self.first_digit_index + self.n_digits - 1] & 1 != 0 {
                            self.roundup();
                        }
                    }
                    std::cmp::Ordering::Greater => self.roundup(),
                    std::cmp::Ordering::Less => {}
                }
            } else {
                self.roundup();
            }
        }
    }

    /// Adds one to the least significant digit; a full carry-out leaves a high-order 1 with a
    /// larger exponent (e.g. `(float) 1e-44`).
    fn roundup(&mut self) {
        let mut i = self.first_digit_index + self.n_digits - 1;
        let mut q = self.digits[i];
        if q == b'9' {
            while q == b'9' && i > self.first_digit_index {
                self.digits[i] = b'0';
                i -= 1;
                q = self.digits[i];
            }
            if q == b'9' {
                self.dec_exponent += 1;
                self.digits[self.first_digit_index] = b'1';
                return;
            }
        }
        self.digits[i] = q + 1;
    }

    fn get_chars(&self, result: &mut Vec<u8>) {
        debug_assert!(self.n_digits <= 19);
        if self.is_negative {
            result.push(b'-');
        }
        let digits = &self.digits[self.first_digit_index..self.first_digit_index + self.n_digits];
        if self.dec_exponent > 0 && self.dec_exponent < 8 {
            // Print digits.digits.
            let dec_exponent = self.dec_exponent as usize;
            let char_length = self.n_digits.min(dec_exponent);
            result.extend_from_slice(&digits[..char_length]);
            if char_length < dec_exponent {
                result.resize(result.len() + dec_exponent - char_length, b'0');
                result.extend_from_slice(b".0");
            } else {
                result.push(b'.');
                if char_length < self.n_digits {
                    result.extend_from_slice(&digits[char_length..]);
                } else {
                    result.push(b'0');
                }
            }
        } else if self.dec_exponent <= 0 && self.dec_exponent > -3 {
            result.extend_from_slice(b"0.");
            result.resize(result.len() + (-self.dec_exponent) as usize, b'0');
            result.extend_from_slice(digits);
        } else {
            result.push(digits[0]);
            result.push(b'.');
            if self.n_digits > 1 {
                result.extend_from_slice(&digits[1..]);
            } else {
                result.push(b'0');
            }
            result.push(b'E');
            let e = if self.dec_exponent <= 0 {
                result.push(b'-');
                -self.dec_exponent + 1
            } else {
                self.dec_exponent - 1
            };
            // The exponent has 1, 2, or 3 digits.
            if e <= 9 {
                result.push(b'0' + e as u8);
            } else if e <= 99 {
                result.push(b'0' + (e / 10) as u8);
                result.push(b'0' + (e % 10) as u8);
            } else {
                result.push(b'0' + (e / 100) as u8);
                result.push(b'0' + (e / 10 % 10) as u8);
                result.push(b'0' + (e % 10) as u8);
            }
        }
    }
}

/// Estimates the decimal exponent: scale the mantissa bits so `1 <= d2 < 2`, approximate
/// `log10(d) ~ (d2-1.5)/1.5 + log10(1.5) + bin_exp * log10(2)`, and take the floor.
// Java's exact literals are load-bearing: a "more precise" log10(2) could change an estimate
// and with it the emitted digits.
#[allow(clippy::approx_constant)]
fn estimate_dec_exp(fract_bits: u64, bin_exp: i32) -> i32 {
    let d2 = f64::from_bits(EXP_ONE | (fract_bits & SIGNIF_BIT_MASK));
    let d = (d2 - 1.5) * 0.289529654 + 0.176091259 + f64::from(bin_exp) * 0.301029995663981;
    let d_bits = d.to_bits();
    let exponent = ((d_bits & EXP_BIT_MASK) >> EXP_SHIFT) as i32 - EXP_BIAS;
    let is_negative = (d_bits & SIGN_BIT_MASK) != 0;
    if (0..52).contains(&exponent) {
        let mask = SIGNIF_BIT_MASK >> exponent;
        let r = (((d_bits & SIGNIF_BIT_MASK) | FRACT_HOB) >> (EXP_SHIFT - exponent)) as i32;
        if is_negative {
            if mask & d_bits == 0 {
                -r
            } else {
                -r - 1
            }
        } else {
            r
        }
    } else if exponent < 0 {
        if d_bits & !SIGN_BIT_MASK == 0 || !is_negative {
            0
        } else {
            -1
        }
    } else {
        d as i32
    }
}

/// `insignificantDigitsForPow2(v) == number of base-10 digits that 1<<v spans below 10`.
fn insignificant_digits_for_pow2(p2: i32) -> i32 {
    if p2 > 1 && (p2 as usize) < INSIGNIFICANT_DIGITS_NUMBER.len() {
        INSIGNIFICANT_DIGITS_NUMBER[p2 as usize]
    } else {
        0
    }
}

#[rustfmt::skip]
const INSIGNIFICANT_DIGITS_NUMBER: [i32; 64] = [
    0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3,
    4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7,
    8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 11, 11, 11,
    12, 12, 12, 12, 13, 13, 13, 14, 14, 14,
    15, 15, 15, 15, 16, 16, 16, 17, 17, 17,
    18, 18, 18, 19,
];

/// Approximately `ceil(log2(LONG_5_POW[i]))`.
#[rustfmt::skip]
const N_5_BITS: [i32; 27] = [
    0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31,
    33, 35, 38, 40, 42, 45, 47, 49, 52, 54, 56, 59, 61,
];

const SMALL_5_POW: [u32; 14] = {
    let mut table = [0u32; 14];
    let mut i = 0;
    let mut value = 1u32;
    while i < table.len() {
        table[i] = value;
        value = value.wrapping_mul(5);
        i += 1;
    }
    table
};

const LONG_5_POW: [u64; 27] = {
    let mut table = [0u64; 27];
    let mut i = 0;
    let mut value = 1u64;
    while i < table.len() {
        table[i] = value;
        value *= 5;
        i += 1;
    }
    table
};

/// `jdk.internal.math.FDBigInteger`, restricted to what `dtoa` reaches. Java's immutable/mutable
/// split exists to share its power-of-five cache; every instance here is freshly built, so all
/// operations are the in-place (mutable) variants — the produced values are identical.
struct FdBigInt {
    /// `data[0]` is least significant; words above `n_words` are insignificant.
    data: Vec<u32>,
    /// Number of least-significant zero-padding words inferred below `data`.
    offset: usize,
    n_words: usize,
}

impl FdBigInt {
    fn new(data: Vec<u32>, offset: usize) -> FdBigInt {
        let n_words = data.len();
        let mut result = FdBigInt {
            data,
            offset,
            n_words,
        };
        result.trim_leading_zeros();
        result
    }

    /// `5^p5 * 2^p2`.
    fn value_of_pow_52(p5: i32, p2: i32) -> FdBigInt {
        if p5 == 0 {
            let wordcount = (p2 >> 5) as usize;
            let bitcount = p2 & 0x1f;
            return FdBigInt::new(vec![1 << bitcount], wordcount);
        }
        if p2 == 0 {
            return FdBigInt::big_5_pow(p5);
        }
        if (p5 as usize) < SMALL_5_POW.len() {
            let pow5 = SMALL_5_POW[p5 as usize];
            let wordcount = (p2 >> 5) as usize;
            let bitcount = p2 & 0x1f;
            if bitcount == 0 {
                return FdBigInt::new(vec![pow5], wordcount);
            }
            return FdBigInt::new(vec![pow5 << bitcount, pow5 >> (32 - bitcount)], wordcount);
        }
        let mut result = FdBigInt::big_5_pow(p5);
        result.left_shift(p2);
        result
    }

    /// `value * 5^p5 * 2^p2`.
    fn value_of_mul_pow_52(value: u64, p5: i32, p2: i32) -> FdBigInt {
        let v0 = value as u32;
        let v1 = (value >> 32) as u32;
        let wordcount = (p2 >> 5) as usize;
        let bitcount = p2 & 0x1f;
        if p5 != 0 {
            if (p5 as usize) < SMALL_5_POW.len() {
                let pow5 = u64::from(SMALL_5_POW[p5 as usize]);
                let mut carry = u64::from(v0) * pow5;
                let v0 = carry as u32;
                carry >>= 32;
                carry += u64::from(v1) * pow5;
                let v1 = carry as u32;
                let v2 = (carry >> 32) as u32;
                if bitcount == 0 {
                    return FdBigInt::new(vec![v0, v1, v2], wordcount);
                }
                return FdBigInt::new(
                    vec![
                        v0 << bitcount,
                        (v1 << bitcount) | (v0 >> (32 - bitcount)),
                        (v2 << bitcount) | (v1 >> (32 - bitcount)),
                        v2 >> (32 - bitcount),
                    ],
                    wordcount,
                );
            }
            let pow5 = FdBigInt::big_5_pow(p5);
            let extra = usize::from(p2 != 0);
            let mut r;
            if v1 == 0 {
                r = vec![0u32; pow5.n_words + 1 + extra];
                mult_by_u32(&pow5.data[..pow5.n_words], v0, &mut r);
            } else {
                r = vec![0u32; pow5.n_words + 2 + extra];
                mult_by_u64_halves(&pow5.data[..pow5.n_words], v0, v1, &mut r);
            }
            let mut result = FdBigInt::new(r, pow5.offset);
            result.left_shift(p2);
            return result;
        }
        if p2 != 0 {
            if bitcount == 0 {
                return FdBigInt::new(vec![v0, v1], wordcount);
            }
            return FdBigInt::new(
                vec![
                    v0 << bitcount,
                    (v1 << bitcount) | (v0 >> (32 - bitcount)),
                    v1 >> (32 - bitcount),
                ],
                wordcount,
            );
        }
        FdBigInt::new(vec![v0, v1], 0)
    }

    /// `5^p`, built the way Java's power-of-five cache is (repeated multiplication by 5), so the
    /// word/offset representation matches too.
    fn big_5_pow(p: i32) -> FdBigInt {
        debug_assert!(p >= 0);
        let p = p as usize;
        if p < SMALL_5_POW.len() {
            return FdBigInt::new(vec![SMALL_5_POW[p]], 0);
        }
        let mut result = FdBigInt::new(vec![SMALL_5_POW[SMALL_5_POW.len() - 1]], 0);
        for _ in SMALL_5_POW.len() - 1..p {
            let mut r = vec![0u32; result.n_words + 1];
            mult_by_u32(&result.data[..result.n_words], 5, &mut r);
            result = FdBigInt::new(r, result.offset);
        }
        result
    }

    fn trim_leading_zeros(&mut self) {
        let mut i = self.n_words;
        if i > 0 && self.data[i - 1] == 0 {
            while i > 0 && self.data[i - 1] == 0 {
                i -= 1;
            }
            self.n_words = i;
            if i == 0 {
                self.offset = 0;
            }
        }
    }

    /// The left shift after which the highest word has its 4 high bits zero and the next bit set.
    fn normalization_bias(&self) -> i32 {
        assert!(self.n_words != 0, "zero value cannot be normalized");
        let zeros = self.data[self.n_words - 1].leading_zeros() as i32;
        if zeros < 4 {
            28 + zeros
        } else {
            zeros - 4
        }
    }

    fn left_shift(&mut self, shift: i32) {
        if shift == 0 || self.n_words == 0 {
            return;
        }
        let wordcount = (shift >> 5) as usize;
        let bitcount = shift & 0x1f;
        if bitcount != 0 {
            let anticount = 32 - bitcount;
            if self.data[0] << bitcount == 0 {
                let mut idx = 0;
                let mut prev = self.data[idx];
                while idx < self.n_words - 1 {
                    let mut v = prev >> anticount;
                    prev = self.data[idx + 1];
                    v |= prev << bitcount;
                    self.data[idx] = v;
                    idx += 1;
                }
                let v = prev >> anticount;
                self.data[idx] = v;
                if v == 0 {
                    self.n_words -= 1;
                }
                self.offset += 1;
            } else {
                let mut idx = self.n_words - 1;
                let mut prev = self.data[idx];
                let hi = prev >> anticount;
                if hi != 0 {
                    if self.n_words == self.data.len() {
                        self.data.push(0);
                    }
                    self.data[self.n_words] = hi;
                    self.n_words += 1;
                }
                while idx > 0 {
                    let mut v = prev << bitcount;
                    prev = self.data[idx - 1];
                    v |= prev >> anticount;
                    self.data[idx] = v;
                    idx -= 1;
                }
                self.data[0] = prev << bitcount;
            }
        }
        self.offset += wordcount;
    }

    fn size(&self) -> usize {
        self.n_words + self.offset
    }

    /// One digit-development step: returns `(int) (this / s)` and replaces `this` with
    /// `10 * (this mod s)`. Assumes `s` is normalized and `this` left-shifted accordingly.
    fn quo_rem_iteration(&mut self, s: &FdBigInt) -> i32 {
        let th_size = self.size();
        let s_size = s.size();
        if th_size < s_size {
            // This value is significantly less than s: the quotient is zero, just multiply by 10.
            let p = mult_and_carry_by_10(&mut self.data[..self.n_words]);
            if p != 0 {
                if self.n_words == self.data.len() {
                    self.data.push(p);
                } else {
                    self.data[self.n_words] = p;
                }
                self.n_words += 1;
            } else {
                self.trim_leading_zeros();
            }
            return 0;
        }
        assert!(th_size == s_size, "disparate values");
        // Estimate q from the high-order words; if too big, add s back in (rarely more than once).
        let mut q = u64::from(self.data[self.n_words - 1]) / u64::from(s.data[s.n_words - 1]);
        let diff = self.mult_diff_me(q, s);
        if diff != 0 {
            let t_start = s.offset - self.offset;
            let mut sum = 0u64;
            while sum == 0 {
                for (s_index, t_index) in (t_start..self.n_words).enumerate() {
                    sum += u64::from(self.data[t_index]) + u64::from(s.data[s_index]);
                    self.data[t_index] = sum as u32;
                    sum >>= 32;
                }
                debug_assert!(sum <= 1);
                q -= 1;
            }
        }
        let p = mult_and_carry_by_10(&mut self.data[..self.n_words]);
        debug_assert!(p == 0);
        self.trim_leading_zeros();
        q as i32
    }

    fn mult_by_10(&mut self) {
        if self.n_words == 0 {
            return;
        }
        let p = mult_and_carry_by_10(&mut self.data[..self.n_words]);
        if p != 0 {
            if self.n_words == self.data.len() {
                if self.data[0] == 0 {
                    self.data.copy_within(1..self.n_words, 0);
                    self.n_words -= 1;
                    self.offset += 1;
                } else {
                    self.data.push(0);
                }
            }
            self.data[self.n_words] = p;
            self.n_words += 1;
        } else {
            self.trim_leading_zeros();
        }
    }

    /// `this - q * s`, in place; returns the borrow.
    fn mult_diff_me(&mut self, q: u64, s: &FdBigInt) -> i64 {
        let mut diff = 0i64;
        if q == 0 {
            return diff;
        }
        let delta_size = s.offset as isize - self.offset as isize;
        if delta_size >= 0 {
            let delta_size = delta_size as usize;
            for s_index in 0..s.n_words {
                let t_index = delta_size + s_index;
                diff = diff
                    .wrapping_add(i64::from(self.data[t_index]))
                    .wrapping_sub((q as i64).wrapping_mul(i64::from(s.data[s_index])));
                self.data[t_index] = diff as u32;
                diff >>= 32;
            }
        } else {
            let delta_size = (-delta_size) as usize;
            let mut rd = vec![0u32; self.n_words + delta_size];
            let mut s_index = 0;
            let mut r_index = 0;
            while r_index < delta_size && s_index < s.n_words {
                diff = diff.wrapping_sub((q as i64).wrapping_mul(i64::from(s.data[s_index])));
                rd[r_index] = diff as u32;
                diff >>= 32;
                s_index += 1;
                r_index += 1;
            }
            let mut t_index = 0;
            while s_index < s.n_words {
                diff = diff
                    .wrapping_add(i64::from(self.data[t_index]))
                    .wrapping_sub((q as i64).wrapping_mul(i64::from(s.data[s_index])));
                rd[r_index] = diff as u32;
                diff >>= 32;
                s_index += 1;
                t_index += 1;
                r_index += 1;
            }
            self.n_words += delta_size;
            self.offset -= delta_size;
            self.data = rd;
        }
        diff
    }

    fn cmp(&self, other: &FdBigInt) -> i32 {
        let a_size = self.size();
        let b_size = other.size();
        if a_size != b_size {
            return if a_size > b_size { 1 } else { -1 };
        }
        let mut a_len = self.n_words;
        let mut b_len = other.n_words;
        while a_len > 0 && b_len > 0 {
            a_len -= 1;
            b_len -= 1;
            let a = self.data[a_len];
            let b = other.data[b_len];
            if a != b {
                return if a < b { -1 } else { 1 };
            }
        }
        if a_len > 0 {
            return check_zero_tail(&self.data, a_len);
        }
        if b_len > 0 {
            return -check_zero_tail(&other.data, b_len);
        }
        0
    }

    /// Compares `this` with `x + y` without materializing the sum when the top words decide it.
    fn add_and_cmp(&self, x: &FdBigInt, y: &FdBigInt) -> i32 {
        let (big, small) = if x.size() >= y.size() { (x, y) } else { (y, x) };
        let b_size = big.size();
        let s_size = small.size();
        let th_size = self.size();
        if b_size == 0 {
            return if th_size == 0 { 0 } else { 1 };
        }
        if s_size == 0 {
            return self.cmp(big);
        }
        if b_size > th_size {
            return -1;
        }
        if b_size + 1 < th_size {
            return 1;
        }
        let mut top = u64::from(big.data[big.n_words - 1]);
        if s_size == b_size {
            top += u64::from(small.data[small.n_words - 1]);
        }
        if top >> 32 == 0 {
            if (top + 1) >> 32 == 0 {
                // No carry extension.
                if b_size < th_size {
                    return 1;
                }
                let v = u64::from(self.data[self.n_words - 1]);
                if v < top {
                    return -1;
                }
                if v > top + 1 {
                    return 1;
                }
            }
        } else {
            // Guaranteed carry extension.
            if b_size + 1 > th_size {
                return -1;
            }
            top >>= 32;
            let v = u64::from(self.data[self.n_words - 1]);
            if v < top {
                return -1;
            }
            if v > top + 1 {
                return 1;
            }
        }
        self.cmp(&big.add(small))
    }

    fn add(&self, other: &FdBigInt) -> FdBigInt {
        let (big, small) = if self.size() >= other.size() {
            (self, other)
        } else {
            (other, self)
        };
        let big_len = big.size();
        let small_len = small.size();
        let mut r = vec![0u32; big_len + 1];
        let word = |value: &FdBigInt, i: usize| {
            if i < value.offset {
                0u64
            } else {
                u64::from(value.data[i - value.offset])
            }
        };
        let mut carry = 0u64;
        for (i, r_word) in r.iter_mut().enumerate().take(big_len) {
            carry += word(big, i);
            if i < small_len {
                carry += word(small, i);
            }
            *r_word = carry as u32;
            carry >>= 32;
        }
        r[big_len] = carry as u32;
        FdBigInt::new(r, 0)
    }
}

fn check_zero_tail(data: &[u32], from: usize) -> i32 {
    if data[..from].iter().all(|word| *word == 0) {
        0
    } else {
        1
    }
}

/// Multiplies the big integer by 10 in place; returns the final carry.
fn mult_and_carry_by_10(data: &mut [u32]) -> u32 {
    let mut carry = 0u64;
    for word in data {
        let product = u64::from(*word) * 10 + carry;
        *word = product as u32;
        carry = product >> 32;
    }
    carry as u32
}

fn mult_by_u32(src: &[u32], value: u32, dst: &mut [u32]) {
    let value = u64::from(value);
    let mut carry = 0u64;
    for (i, word) in src.iter().enumerate() {
        let product = u64::from(*word) * value + carry;
        dst[i] = product as u32;
        carry = product >> 32;
    }
    dst[src.len()] = carry as u32;
}

fn mult_by_u64_halves(src: &[u32], v0: u32, v1: u32, dst: &mut [u32]) {
    let mut v = u64::from(v0);
    let mut carry = 0u64;
    for (j, word) in src.iter().enumerate() {
        let product = v * u64::from(*word) + carry;
        dst[j] = product as u32;
        carry = product >> 32;
    }
    dst[src.len()] = carry as u32;
    v = u64::from(v1);
    carry = 0;
    for (j, word) in src.iter().enumerate() {
        let product = u64::from(dst[j + 1]) + v * u64::from(*word) + carry;
        dst[j + 1] = product as u32;
        carry = product >> 32;
    }
    dst[src.len() + 1] = carry as u32;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn double_string(value: f64) -> String {
        let mut out = Vec::new();
        jdk_double_to_string(value, &mut out);
        String::from_utf8(out).expect("ASCII spelling")
    }

    fn float_string(value: f32) -> String {
        let mut out = Vec::new();
        jdk_float_to_string(value, &mut out);
        String::from_utf8(out).expect("ASCII spelling")
    }

    /// Spellings pinned from JDK 17's `Double.toString` (edge values, powers of 10 and 2 across
    /// the plain/scientific boundary, subnormals, and 100 seeded random bit patterns).
    #[rustfmt::skip]
    const PINNED_DOUBLES: &[(u64, &str)] = &[
    (0x7FF8000000000000, "NaN"),
    (0x7FF0000000000000, "Infinity"),
    (0xFFF0000000000000, "-Infinity"),
    (0x0000000000000000, "0.0"),
    (0x8000000000000000, "-0.0"),
    (0x0000000000000001, "4.9E-324"),
    (0x8000000000000001, "-4.9E-324"),
    (0x7FEFFFFFFFFFFFFF, "1.7976931348623157E308"),
    (0xFFEFFFFFFFFFFFFF, "-1.7976931348623157E308"),
    (0x0010000000000000, "2.2250738585072014E-308"),
    (0x000FFFFFFFFFFFFF, "2.225073858507201E-308"),
    (0x3FB999999999999A, "0.1"),
    (0xBFB999999999999A, "-0.1"),
    (0x3FD5555555555555, "0.3333333333333333"),
    (0x3FE5555555555555, "0.6666666666666666"),
    (0x400921FB54442D18, "3.141592653589793"),
    (0x4005BF0A8B145769, "2.718281828459045"),
    (0x433FFFFFFFFFFFFF, "9.007199254740991E15"),
    (0x4340000000000000, "9.007199254740992E15"),
    (0x3FF8000000000000, "1.5"),
    (0x4059000000000000, "100.0"),
    (0x40C81CD6C8B43958, "12345.678"),
    (0x0000000000000002, "1.0E-323"),
    (0x39B4484BFEEBC2A0, "1.0E-30"),
    (0x39E95A5EFEA6B348, "1.0000000000000001E-29"),
    (0x3A1FB0F6BE506019, "1.0E-28"),
    (0x3A53CE9A36F23C10, "1.0E-27"),
    (0x3A88C240C4AECB14, "1.0E-26"),
    (0x3ABEF2D0F5DA7DD9, "1.0E-25"),
    (0x3AF357C299A88EA8, "1.0000000000000001E-24"),
    (0x3B282DB34012B251, "1.0E-23"),
    (0x3B5E392010175EE6, "1.0E-22"),
    (0x3B92E3B40A0E9B50, "1.0000000000000001E-21"),
    (0x3BC79CA10C924224, "1.0000000000000001E-20"),
    (0x3BFD83C94FB6D2AC, "1.0E-19"),
    (0x3C32725DD1D243AC, "1.0E-18"),
    (0x3C670EF54646D496, "9.999999999999999E-18"),
    (0x3C9CD2B297D889BC, "1.0E-16"),
    (0x3CD203AF9EE75616, "1.0E-15"),
    (0x3D06849B86A12B9B, "1.0E-14"),
    (0x3D3C25C268497682, "1.0E-13"),
    (0x3D719799812DEA11, "1.0E-12"),
    (0x3DA5FD7FE1796496, "1.0000000000000001E-11"),
    (0x3DDB7CDFD9D7BDBB, "1.0E-10"),
    (0x3E112E0BE826D695, "1.0E-9"),
    (0x3E45798EE2308C3A, "1.0E-8"),
    (0x3E7AD7F29ABCAF48, "1.0E-7"),
    (0x3EB0C6F7A0B5ED8D, "1.0E-6"),
    (0x3EE4F8B588E368F0, "9.999999999999999E-6"),
    (0x3F1A36E2EB1C432D, "1.0E-4"),
    (0x3F50624DD2F1A9FC, "0.001"),
    (0x3F847AE147AE147B, "0.01"),
    (0x3FF0000000000000, "1.0"),
    (0x4024000000000000, "10.0"),
    (0x408F400000000000, "1000.0"),
    (0x40C3880000000000, "10000.0"),
    (0x40F86A0000000000, "100000.0"),
    (0x412E848000000000, "1000000.0"),
    (0x416312D000000000, "1.0E7"),
    (0x4197D78400000000, "1.0E8"),
    (0x41CDCD6500000000, "1.0E9"),
    (0x4202A05F20000000, "1.0E10"),
    (0x42374876E8000000, "1.0E11"),
    (0x426D1A94A2000000, "1.0E12"),
    (0x42A2309CE5400000, "1.0E13"),
    (0x42D6BCC41E900000, "1.0E14"),
    (0x430C6BF526340000, "1.0E15"),
    (0x4341C37937E08000, "1.0E16"),
    (0x4376345785D8A000, "1.0E17"),
    (0x43ABC16D674EC800, "1.0E18"),
    (0x43E158E460913D00, "1.0E19"),
    (0x4415AF1D78B58C40, "1.0E20"),
    (0x444B1AE4D6E2EF50, "1.0E21"),
    (0x4480F0CF064DD592, "1.0E22"),
    (0x44B52D02C7E14AF6, "9.999999999999999E22"),
    (0x44EA784379D99DB4, "1.0E24"),
    (0x45208B2A2C280291, "1.0E25"),
    (0x4554ADF4B7320335, "1.0E26"),
    (0x4589D971E4FE8402, "1.0E27"),
    (0x45C027E72F1F1281, "1.0E28"),
    (0x45F431E0FAE6D722, "1.0000000000000001E29"),
    (0x46293E5939A08CEA, "1.0E30"),
    (0x0000020000000000, "1.0864618449742E-311"),
    (0x01F0000000000000, "2.3891548633682403E-299"),
    (0x0480000000000000, "5.2538071056619216E-287"),
    (0x0710000000000000, "1.1553244005534909E-274"),
    (0x09A0000000000000, "2.5405852245238005E-262"),
    (0x0C30000000000000, "5.5868059914396366E-250"),
    (0x0EC0000000000000, "1.2285516299433009E-237"),
    (0x1150000000000000, "2.7016136048916335E-225"),
    (0x13E0000000000000, "5.9409111446723744E-213"),
    (0x1670000000000000, "1.3064201766302604E-200"),
    (0x1900000000000000, "2.872848349932294E-188"),
    (0x1B90000000000000, "6.3174603311753045E-176"),
    (0x1E20000000000000, "1.3892242184281734E-163"),
    (0x20B0000000000000, "3.0549363634996047E-151"),
    (0x2340000000000000, "6.717876107567089E-139"),
    (0x25D0000000000000, "1.4772765788457177E-126"),
    (0x2860000000000000, "3.248565551764031E-114"),
    (0x2AF0000000000000, "7.143671195514219E-102"),
    (0x2D80000000000000, "1.5709099088952725E-89"),
    (0x3010000000000000, "3.454467422037778E-77"),
    (0x32A0000000000000, "7.596454196607839E-65"),
    (0x3530000000000000, "1.6704779438076223E-52"),
    (0x37C0000000000000, "3.6734198463196485E-40"),
    (0x3A50000000000000, "8.077935669463161E-28"),
    (0x3CE0000000000000, "1.7763568394002505E-15"),
    (0x3F70000000000000, "0.00390625"),
    (0x4200000000000000, "8.589934592E9"),
    (0x4490000000000000, "1.888946593147858E22"),
    (0x4720000000000000, "4.153837486827862E34"),
    (0x49B0000000000000, "9.134385233318143E46"),
    (0x4C40000000000000, "2.0086725553237378E59"),
    (0x4ED0000000000000, "4.417117661945961E71"),
    (0x5160000000000000, "9.713344461128645E83"),
    (0x53F0000000000000, "2.13598703592091E96"),
    (0x5680000000000000, "4.6970851655476665E108"),
    (0x5910000000000000, "1.0328999512347634E121"),
    (0x5BA0000000000000, "2.2713710134237715E133"),
    (0x5E30000000000000, "4.9947976805055876E145"),
    (0x60C0000000000000, "1.0983676256208976E158"),
    (0x6350000000000000, "2.4153359518857865E170"),
    (0x65E0000000000000, "5.311379928167671E182"),
    (0x6870000000000000, "1.167984798111282E195"),
    (0x6B00000000000000, "2.5684257331779168E207"),
    (0x6D90000000000000, "5.648027917416435E219"),
    (0x7020000000000000, "1.2420144738405671E232"),
    (0x72B0000000000000, "2.7312187117075883E244"),
    (0x7540000000000000, "6.00601346304376E256"),
    (0x77D0000000000000, "1.3207363278391631E269"),
    (0x7A60000000000000, "2.90432989937067E281"),
    (0x7CF0000000000000, "6.3866889905111034E293"),
    (0x7F80000000000000, "1.4044477616111843E306"),
    (0xD1732A96DA77E911, "-2.3271187362849265E84"),
    (0x095344EC346F9FDB, "9.561546678240826E-264"),
    (0x86FD317EAFB0DC46, "-5.269965197017032E-275"),
    (0x8F3031C368B7742B, "-1.5916513374895473E-235"),
    (0xF83099F99A35CD1A, "-8.770463105007374E270"),
    (0xB58CFE754A17CA2B, "-9.686759881369103E-51"),
    (0x941E3EE7D02CC080, "-8.984358144017975E-212"),
    (0xE839A407AA75976B, "-1.1698436263761756E194"),
    (0x138303CD985D948C, "1.103179387034571E-214"),
    (0x0F043B06B6CB4170, "2.4854301220114494E-236"),
    (0x5D69CF20D8B2A76B, "9.83514462123632E141"),
    (0x516F495D9BF29FCB, "1.8993585834647743E84"),
    (0x717C64C568A87994, "4.622294501512304E238"),
    (0x1BE48A8916448B66, "2.595358546581252E-174"),
    (0xE328E4D004D8BF3D, "-4.697413123932368E169"),
    (0x728995ECDFD18F20, "5.4593745461678424E243"),
    (0xD6D9F29202A0950F, "-2.4375561456447383E110"),
    (0x704E0785311C8D9C, "9.324229937977094E232"),
    (0x39DBC7D8A6A8CC1C, "5.478767094242699E-30"),
    (0x30210EC09D797056, "7.365626912018253E-77"),
    (0x6668B86FF2128110, "2.100795924086783E185"),
    (0xB4B687AA3BBF33F6, "-9.188420416464449E-55"),
    (0xA3FBE9E8F77972A6, "-2.400266911578136E-135"),
    (0x7F220248971DE5E8, "2.469979255836228E304"),
    (0x10B1439C2620CB5E, "2.8467365235268666E-228"),
    (0xD8105C4977DE672C, "-1.611590915499548E116"),
    (0xD765DAEF3DB6526C, "-1.0511953603381395E113"),
    (0xB0CED0DB69DB086A, "-1.3625899896857365E-73"),
    (0x07BC5FD5C36337F3, "2.0980214579209047E-271"),
    (0xEAD8A2D88BDD5349, "-4.943440327170798E206"),
    (0x955D4D9D48A0F072, "-9.127203743635431E-206"),
    (0xA492C52DC8FE9500, "-1.6527665242516053E-132"),
    (0x9B02E3E4C0BBD37E, "-1.456764496243026E-178"),
    (0x626F257418EDFE2C, "1.4348751986230699E166"),
    (0xE85D2BEA9A5471B1, "-5.32373831120204E194"),
    (0xF3277CDDCED86BE4, "-5.132016664059386E246"),
    (0x8FDA8E0FA1CBD5E2, "-2.672566079637697E-232"),
    (0x4E59CB56B5DB2E0A, "2.7816509298682627E69"),
    (0xCF25BB9497962596, "-1.9199210481612753E73"),
    (0x54CEE0EAAEAC5EE4, "3.376959930124476E100"),
    (0x82F1F0A5553254B5, "-1.7556067397522727E-294"),
    (0xDB55996679B89FEA, "-9.582000390065293E131"),
    (0xD8BAD297BC3CE080, "-2.7055752874516644E119"),
    (0x3AA3AE3A2C9F4FE1, "3.1795684031390775E-26"),
    (0x1FAD6E7517FEC5C2, "4.2873039622825565E-156"),
    (0x5A5742A4AEF38E28, "1.574540996750671E127"),
    (0x39441BB922392645, "7.745432980314933E-33"),
    (0xD1428E1341B99F51, "-2.8161082058114686E83"),
    (0x0277475643015187, "8.898611800148056E-297"),
    (0xE3DD48C01C7D86F2, "-1.1316981779571352E173"),
    (0x2C016AFDA08824AE, "1.019314036623163E-96"),
    (0x694DBEA13B58EABD, "1.7787543726897767E199"),
    (0xEB54B4A011E2F5B8, "-1.0636142463218461E209"),
    (0x9A35AC83900B92CE, "-2.040324998423506E-182"),
    (0x0AC52232669B7DF6, "8.796842400274945E-257"),
    (0x908D4B7576AB1B44, "-6.038141261587689E-229"),
    (0x7E5215B0C72F77BA, "3.027803504332962E300"),
    (0x7F992E63F9A3AB12, "4.420712279024561E306"),
    (0x5FA6A5653FFFE34F, "5.930356251722142E152"),
    (0xFCADD603920CC416, "-3.721742216991697E292"),
    (0x2F982C690BBB2729, "2.0387327096299604E-79"),
    (0xCF3E1EF575F2806B, "-5.321908194198621E73"),
    (0x59DEDF699B4C7F5D, "8.163434412612961E124"),
    (0xC6DFEE238F64D0FB, "-2.5904880228776972E33"),
    (0x213C69AE0B0E7052, "1.3887893180738903E-148"),
    (0xCAC4BF721A239157, "-1.5525373207460122E52"),
    (0x7B00D65806A530B8, "3.129665131046548E284"),
    (0x5DD9FA92EBD784CA, "1.267176481075274E144"),
    (0xA91E49E999C13EB9, "-1.2594546523284482E-110"),
    (0xF79F3238313590AA, "-1.6094498011628782E268"),
    (0x0600500A1359946F, "8.986667110934217E-280"),
    (0x01FEB170226AC7F2, "4.583163152205531E-299"),
    (0xED63C187A5F02F95, "-8.717367532805248E218"),
    (0xDA56FB4AE0051C0D, "-1.5556742442215927E127"),
    (0xB5B8869F796AE080, "-6.555188218438847E-50"),
    (0x95AA20C06875C6F4, "-2.604222309226799E-204"),
    (0x8666B97A4D35944F, "-8.012144545811969E-278"),
    (0x57D210CFD287C27A, "1.112224508328629E115"),
    (0x84E4ECF9E3028E61, "-4.397581125764382E-285"),
    (0x09E746E9B3F5E0E6, "5.913721122510384E-261"),
    (0x5D259BB636D04507, "5.1464477809972065E140"),
    (0x06F97B90A9F15AC2, "4.6001183658947333E-275"),
    (0xDDBE0341FF23D57A, "-3.659850905900487E143"),
    (0xB6103B3B4522E5F2, "-2.776489044693003E-48"),
    (0x0D88EFB43BE2430F, "1.8260232177301925E-243"),
    (0xE763877C60BDAA07, "-1.0876587540868719E190"),
    (0x5BAC3B645A21F5A8, "4.0078341533610147E133"),
    (0x771AE663FDDCA5D5, "5.42110575618736E265"),
    (0xEC1006F1FAF12B6E, "-3.372195239750719E212"),
    (0x23E38423BED85A42, "8.390861763981193E-136"),
    (0x65C2ED8C01A551BC, "1.5708337735750163E182"),
    (0x89944AF6CACBCE60, "-1.6111143629683386E-262"),
    (0x6EB6B96FAF12795E, "2.102850715731018E225"),
    (0xC221153E455D40BE, "-3.668542327862645E10"),
    (0x4CF55CB0294959FA, "5.4924018801602105E62"),
    (0xAF8C5DED435019FA, "-1.1961979666096569E-79"),
    (0x21263233866D98CE, "5.424613526351595E-149"),
    (0x62991325AEAAF49B, "9.241340758847309E166"),
    (0x4537CB04DC1D469B, "2.87640238950464E25"),
    (0x59F46F226E95CE1D, "2.1612954279379766E125"),
    ];

    /// Spellings pinned from JDK 17's `Float.toString`.
    #[rustfmt::skip]
    const PINNED_FLOATS: &[(u32, &str)] = &[
    (0x7FC00000, "NaN"),
    (0x7F800000, "Infinity"),
    (0xFF800000, "-Infinity"),
    (0x00000000, "0.0"),
    (0x80000000, "-0.0"),
    (0x00000001, "1.4E-45"),
    (0x80000001, "-1.4E-45"),
    (0x7F7FFFFF, "3.4028235E38"),
    (0xFF7FFFFF, "-3.4028235E38"),
    (0x00800000, "1.17549435E-38"),
    (0x007FFFFF, "1.1754942E-38"),
    (0x3DCCCCCD, "0.1"),
    (0xBDCCCCCD, "-0.1"),
    (0x3EAAAAAB, "0.33333334"),
    (0x3F2AAAAB, "0.6666667"),
    (0x40490FDB, "3.1415927"),
    (0x3FC00000, "1.5"),
    (0x42C80000, "100.0"),
    (0x4640E6B6, "12345.678"),
    (0x00000007, "9.8E-45"),
    (0x4B7FFFFF, "1.6777215E7"),
    (0x4B800000, "1.6777216E7"),
    (0x1E3CE508, "1.0E-20"),
    (0x1FEC1E4A, "1.0E-19"),
    (0x219392EF, "1.0E-18"),
    (0x233877AA, "1.0E-17"),
    (0x24E69595, "1.0E-16"),
    (0x26901D7D, "1.0E-15"),
    (0x283424DC, "1.0E-14"),
    (0x29E12E13, "1.0E-13"),
    (0x2B8CBCCC, "1.0E-12"),
    (0x2D2FEBFF, "1.0E-11"),
    (0x2EDBE6FF, "1.0E-10"),
    (0x3089705F, "1.0E-9"),
    (0x322BCC77, "1.0E-8"),
    (0x33D6BF95, "1.0E-7"),
    (0x358637BD, "1.0E-6"),
    (0x3727C5AC, "1.0E-5"),
    (0x38D1B717, "1.0E-4"),
    (0x3A83126F, "0.001"),
    (0x3C23D70A, "0.01"),
    (0x3F800000, "1.0"),
    (0x41200000, "10.0"),
    (0x447A0000, "1000.0"),
    (0x461C4000, "10000.0"),
    (0x47C35000, "100000.0"),
    (0x49742400, "1000000.0"),
    (0x4B189680, "1.0E7"),
    (0x4CBEBC20, "1.0E8"),
    (0x4E6E6B28, "1.0E9"),
    (0x501502F9, "1.0E10"),
    (0x51BA43B7, "9.9999998E10"),
    (0x5368D4A5, "1.0E12"),
    (0x551184E7, "9.9999998E12"),
    (0x56B5E621, "1.0E14"),
    (0x58635FA9, "9.9999999E14"),
    (0x5A0E1BCA, "1.00000003E16"),
    (0x5BB1A2BC, "9.9999998E16"),
    (0x5D5E0B6B, "9.9999998E17"),
    (0x5F0AC723, "1.0E19"),
    (0x60AD78EC, "1.0E20"),
    (0x00000200, "7.175E-43"),
    (0x00040000, "3.67342E-40"),
    (0x02800000, "1.880791E-37"),
    (0x07000000, "9.62965E-35"),
    (0x0B800000, "4.9303807E-32"),
    (0x10000000, "2.5243549E-29"),
    (0x14800000, "1.2924697E-26"),
    (0x19000000, "6.617445E-24"),
    (0x1D800000, "3.3881318E-21"),
    (0x22000000, "1.7347235E-18"),
    (0x26800000, "8.881784E-16"),
    (0x2B000000, "4.5474735E-13"),
    (0x2F800000, "2.3283064E-10"),
    (0x34000000, "1.1920929E-7"),
    (0x38800000, "6.1035156E-5"),
    (0x3D000000, "0.03125"),
    (0x41800000, "16.0"),
    (0x46000000, "8192.0"),
    (0x4A800000, "4194304.0"),
    (0x4F000000, "2.14748365E9"),
    (0x53800000, "1.09951163E12"),
    (0x58000000, "5.6294995E14"),
    (0x5C800000, "2.88230376E17"),
    (0x61000000, "1.4757395E20"),
    (0x65800000, "7.5557864E22"),
    (0x6A000000, "3.8685626E25"),
    (0x6E800000, "1.9807041E28"),
    (0x73000000, "1.0141205E31"),
    (0x77800000, "5.192297E33"),
    (0x7C000000, "2.658456E36"),
    (0x7C82CBC8, "5.433054E36"),
    (0xBEFDC8E0, "-0.49567318"),
    (0x7EA9FB58, "1.1297229E38"),
    (0x3CC1B54C, "0.02364602"),
    (0x84FE9E87, "-5.9860697E-36"),
    (0x4691A24B, "18641.146"),
    (0x3B3221D7, "0.0027180815"),
    (0x4D6C89D5, "2.48028496E8"),
    (0xF368FCB7, "-1.8459145E31"),
    (0x03BDCE6C, "1.1155814E-36"),
    (0xE48DF0F7, "-2.0946834E22"),
    (0x9528A119, "-3.4054413E-26"),
    (0x12CD9576, "1.2974164E-27"),
    (0x4B9CA4AB, "2.0531542E7"),
    (0xDEE8F530, "-8.393188E18"),
    (0x61B96444, "4.2748407E20"),
    (0x42EB5413, "117.66421"),
    (0xDFA7767C, "-2.4133937E19"),
    (0x8B644D49, "-4.3969346E-32"),
    (0xD6D33B9E, "-1.16126504E14"),
    (0x4CC90AD3, "1.05404056E8"),
    (0xE71F5CD0, "-7.5256836E23"),
    (0xA13C5CB4, "-6.381957E-19"),
    (0xED9E5B20, "-6.1260994E27"),
    (0xA1A385F2, "-1.1080765E-18"),
    (0x9F30E8CE, "-3.746202E-20"),
    (0x3BACC038, "0.0052719377"),
    (0xEBC1444A, "-4.6729034E26"),
    (0x027B4AE8, "1.8462065E-37"),
    (0x56D5ABC2, "1.17466835E14"),
    (0xA60C68D0, "-4.8714305E-16"),
    (0x0DB11133, "1.0912608E-30"),
    (0xB62887EC, "-2.5113068E-6"),
    (0xA431CA55, "-3.855214E-17"),
    (0xF2D600D3, "-8.477541E30"),
    (0xD552DCE1, "-1.44903818E13"),
    (0x0D3FF7BD, "5.9154623E-31"),
    (0x225B0EFE, "2.9687971E-18"),
    (0x3EEEFF54, "0.46679175"),
    (0x22DADCDA, "5.9322854E-18"),
    (0x567BBD9F, "6.9197959E13"),
    (0x035A2FB8, "6.411922E-37"),
    (0x5AC20DCB, "2.73106555E16"),
    (0xD78AFFFB, "-3.05664065E14"),
    (0xB0077DA3, "-4.9291254E-10"),
    (0xE20E3AF5, "-6.559215E20"),
    (0x84E7A3F4, "-5.4458406E-36"),
    (0x39BC2687, "3.588686E-4"),
    (0xF23F352C, "-3.7872587E30"),
    (0x3DA55FD2, "0.08074917"),
    (0x2861FDF6, "1.2545078E-14"),
    (0xF4BA4782, "-1.1806855E32"),
    (0xAB6497AD, "-8.1212364E-13"),
    (0x13DD858B, "5.5919927E-27"),
    (0xC91FD0C8, "-654604.5"),
    (0x3EAD1740, "0.338068"),
    (0x31C6F3B7, "5.7902656E-9"),
    (0x0B726630, "4.6684382E-32"),
    (0xB6CB60D0, "-6.061142E-6"),
    (0x204C0338, "1.7280537E-19"),
    (0xDF15E432, "-1.0800813E19"),
    (0x64462098, "1.4619216E22"),
    (0xE621DECC, "-1.9110272E23"),
    (0xD6B5971D, "-9.9830316E13"),
    (0xC45751EA, "-861.2799"),
    (0x7A2C7596, "2.23865E35"),
    (0xBE692EF5, "-0.22771819"),
    (0xECAF9F2F, "-1.69851E27"),
    (0x867D894D, "-4.7684884E-35"),
    (0x9110246A, "-1.1370818E-28"),
    (0x69587E61, "1.6357799E25"),
    (0xA7169BCD, "-2.0901141E-15"),
    (0x90226872, "-3.2029328E-29"),
    (0x10AD1A7A, "6.827726E-29"),
    (0x9A2A4AA8, "-3.5215483E-23"),
    (0x3D985A4F, "0.074391"),
    (0x99B8E95D, "-1.9119409E-23"),
    (0x72916A42, "5.7604844E30"),
    (0x02D4BFCA, "3.1260682E-37"),
    (0xAE3EC712, "-4.337781E-11"),
    (0x76D68C81, "2.1757838E33"),
    (0x49C23D87, "1591216.9"),
    (0x26CF9050, "1.4402626E-15"),
    (0xB50D4CD3, "-5.263839E-7"),
    (0xA14A6785, "-6.857727E-19"),
    (0xC78136D2, "-66157.64"),
    (0xA79CABB0, "-4.348484E-15"),
    (0x12445540, "6.1951774E-28"),
    (0xA9CFDC4B, "-9.2308614E-14"),
    (0x7781F61C, "5.271859E33"),
    (0xEABB76F5, "-1.1331544E26"),
    (0x2703BECC, "1.8283333E-15"),
    (0x56873C2D, "7.4346261E13"),
    (0x16A6B4C5, "2.6932828E-25"),
    (0x59046B9F, "2.32956422E15"),
    (0x723E5BC2, "3.770437E30"),
    (0x193EDB89, "9.8671045E-24"),
    (0x0D562CCB, "6.599776E-31"),
    (0xDEE540C7, "-8.2597111E18"),
    (0x5C7DCE9D, "2.85761371E17"),
    ];

    #[test]
    fn doubles_spell_like_jdk_17() {
        for (bits, expected) in PINNED_DOUBLES {
            assert_eq!(
                &double_string(f64::from_bits(*bits)),
                expected,
                "bits {bits:#018X}"
            );
        }
    }

    #[test]
    fn floats_spell_like_jdk_17() {
        for (bits, expected) in PINNED_FLOATS {
            assert_eq!(
                &float_string(f32::from_bits(*bits)),
                expected,
                "bits {bits:#010X}"
            );
        }
    }

    /// The legacy algorithm's digits are not always the shortest, but they always parse back to
    /// the exact value ("must accurately represent the value"). A wide seeded sweep pins that
    /// self-consistency; byte parity itself is pinned above and by the Java referee suite.
    #[test]
    fn spellings_round_trip_over_a_seeded_sweep() {
        let mut state = 0x243F_6A88_85A3_08D3u64; // fixed LCG over raw bit patterns
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };
        for _ in 0..200_000 {
            let value = f64::from_bits(next());
            if !value.is_finite() {
                continue;
            }
            let spelled = double_string(value);
            let parsed: f64 = spelled.parse().expect("valid decimal");
            assert_eq!(parsed.to_bits(), value.to_bits(), "double {spelled}");
        }
        for _ in 0..200_000 {
            let value = f32::from_bits(next() as u32);
            if !value.is_finite() {
                continue;
            }
            let spelled = float_string(value);
            let parsed: f32 = spelled.parse().expect("valid decimal");
            assert_eq!(parsed.to_bits(), value.to_bits(), "float {spelled}");
        }
    }
}
