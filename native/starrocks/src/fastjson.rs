//! fastjson 1.2.83 spellings used by the released StarRocks connector's JSON rows and nested
//! values: its default string escaping and its own Ryu ports for `Float`/`Double` (shortest digits
//! in `Double.toString` layout, keeping two digits in scientific notation). NaN and infinities are
//! written as `null` by fastjson's number codecs.
//!
//! The Ryu functions are mechanical ports of `com.alibaba.fastjson.util.RyuDouble`/`RyuFloat`
//! (Copyright 2018 Ulf Adams, Apache License 2.0), keeping Java's wrapping `long`/`int`
//! arithmetic and masked shift distances so every spelling stays byte-identical.

use num_bigint::BigUint;
use std::sync::LazyLock;

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// Writes a quoted JSON string with fastjson's default escapes over valid UTF-8 text: the short
/// escapes for `\b \t \n \f \r " \\`, and `\uXXXX` for the remaining C0 controls, U+007F-U+009F
/// and U+2028/U+2029. Everything else, including `/` and supplementary characters, is verbatim.
pub(crate) fn write_string(text: &[u8], output: &mut Vec<u8>) {
    output.reserve(text.len() + 2);
    output.push(b'"');
    let mut start = 0;
    let mut index = 0;
    while index < text.len() {
        let byte = text[index];
        let escape: Option<(&[u8], usize)> = match byte {
            b'\x08' => Some((b"\\b", 1)),
            b'\t' => Some((b"\\t", 1)),
            b'\n' => Some((b"\\n", 1)),
            b'\x0c' => Some((b"\\f", 1)),
            b'\r' => Some((b"\\r", 1)),
            b'"' => Some((b"\\\"", 1)),
            b'\\' => Some((b"\\\\", 1)),
            0x00..=0x1f | 0x7f => Some((&[], 1)),
            0xc2 if matches!(text.get(index + 1), Some(0x80..=0x9f)) => Some((&[], 2)),
            0xe2 if text.get(index + 1) == Some(&0x80)
                && matches!(text.get(index + 2), Some(0xa8 | 0xa9)) =>
            {
                Some((&[], 3))
            }
            _ => None,
        };
        let Some((short, width)) = escape else {
            index += 1;
            continue;
        };
        output.extend_from_slice(&text[start..index]);
        if short.is_empty() {
            let code = match width {
                1 => u32::from(byte),
                2 => u32::from(text[index + 1] & 0x3f) | 0x80,
                _ => 0x2000 | u32::from(text[index + 2] & 0x3f),
            };
            output.extend_from_slice(b"\\u");
            for shift in [12, 8, 4, 0] {
                output.push(HEX[((code >> shift) & 15) as usize]);
            }
        } else {
            output.extend_from_slice(short);
        }
        index += width;
        start = index;
    }
    output.extend_from_slice(&text[start..]);
    output.push(b'"');
}

/// fastjson's `DoubleSerializer`: `null` for NaN and infinities, otherwise `RyuDouble`.
pub(crate) fn write_double(value: f64, output: &mut Vec<u8>) {
    if !value.is_finite() {
        output.extend_from_slice(b"null");
        return;
    }
    let mut buffer = [0u8; 32];
    let length = ryu_double(value, &mut buffer);
    output.extend_from_slice(&buffer[..length]);
}

/// fastjson's `FloatCodec`: `null` for NaN and infinities, otherwise `RyuFloat`.
pub(crate) fn write_float(value: f32, output: &mut Vec<u8>) {
    if !value.is_finite() {
        output.extend_from_slice(b"null");
        return;
    }
    let mut buffer = [0u8; 32];
    let length = ryu_float(value, &mut buffer);
    output.extend_from_slice(&buffer[..length]);
}

fn java_int_value(value: &BigUint) -> i32 {
    value.iter_u32_digits().next().unwrap_or(0) as i32
}

fn shift_right(value: &BigUint, distance: i64) -> BigUint {
    if distance >= 0 {
        value >> distance as u64
    } else {
        value << distance.unsigned_abs()
    }
}

struct DoubleTables {
    pow5_split: Vec<[i32; 4]>,
    pow5_inv_split: Vec<[i32; 4]>,
}

static DOUBLE_TABLES: LazyLock<DoubleTables> = LazyLock::new(|| {
    let mask = (BigUint::from(1u8) << 31u32) - 1u8;
    let mut pow5_split = Vec::with_capacity(326);
    let mut pow5_inv_split = Vec::with_capacity(291);
    for i in 0..326u32 {
        let pow = BigUint::from(5u8).pow(i);
        let pow5_len = pow.bits() as i64;
        let mut split = [0i32; 4];
        for (j, entry) in split.iter_mut().enumerate() {
            let distance = pow5_len - 121 + (3 - j as i64) * 31;
            *entry = java_int_value(&(shift_right(&pow, distance) & &mask));
        }
        pow5_split.push(split);
        if i < 291 {
            let inverse = (BigUint::from(1u8) << (pow5_len + 121) as u64) / &pow + 1u8;
            let mut split = [0i32; 4];
            for (k, entry) in split.iter_mut().enumerate() {
                let shifted = &inverse >> ((3 - k) * 31);
                *entry = java_int_value(&if k == 0 { shifted } else { shifted & &mask });
            }
            pow5_inv_split.push(split);
        }
    }
    DoubleTables {
        pow5_split,
        pow5_inv_split,
    }
});

fn unsigned_shift(value: i64, distance: i32) -> i64 {
    ((value as u64) >> (distance & 63)) as i64
}

fn pow5_bits(e: i64) -> i64 {
    if e == 0 {
        1
    } else {
        (e * 23_219_280 + 10_000_000 - 1) / 10_000_000
    }
}

fn mul_shift(m: i64, ints: &[i32; 4], shift: i32) -> i64 {
    let high = unsigned_shift(m, 31);
    let low = m & 0x7fff_ffff;
    let bits13 = high.wrapping_mul(i64::from(ints[0]));
    let bits03 = low.wrapping_mul(i64::from(ints[0]));
    let bits12 = high.wrapping_mul(i64::from(ints[1]));
    let bits02 = low.wrapping_mul(i64::from(ints[1]));
    let bits11 = high.wrapping_mul(i64::from(ints[2]));
    let bits01 = low.wrapping_mul(i64::from(ints[2]));
    let bits10 = high.wrapping_mul(i64::from(ints[3]));
    let bits00 = low.wrapping_mul(i64::from(ints[3]));
    let sum = unsigned_shift(bits00, 31)
        .wrapping_add(bits01)
        .wrapping_add(bits10);
    let sum = unsigned_shift(sum, 31)
        .wrapping_add(bits02)
        .wrapping_add(bits11);
    let sum = unsigned_shift(sum, 31)
        .wrapping_add(bits03)
        .wrapping_add(bits12);
    let sum = unsigned_shift(sum, 21).wrapping_add(bits13.wrapping_shl(10));
    unsigned_shift(sum, shift)
}

fn pow5_factor(mut value: i64) -> i32 {
    if value % 5 != 0 {
        return 0;
    }
    if value % 25 != 0 {
        return 1;
    }
    if value % 125 != 0 {
        return 2;
    }
    if value % 625 != 0 {
        return 3;
    }
    let mut count = 4;
    value /= 625;
    while value > 0 {
        if value % 5 != 0 {
            break;
        }
        value /= 5;
        count += 1;
    }
    count
}

fn decimal_length(value: i64) -> i32 {
    let mut length = 1;
    let mut limit = 10i64;
    while length < 19 && value >= limit {
        length += 1;
        if length < 19 {
            limit *= 10;
        }
    }
    length
}

/// Writes the fastjson `RyuDouble` spelling of a finite value; returns the byte length.
fn ryu_double(value: f64, result: &mut [u8; 32]) -> usize {
    const MANTISSA_BITS: i32 = 52;
    const MANTISSA_MASK: i64 = (1 << 52) - 1;
    const EXPONENT_MASK: i64 = 2047;
    const EXPONENT_BIAS: i32 = 1023;
    const LOG10_5_NUMERATOR: i64 = 6_989_700;
    const LOG10_2_NUMERATOR: i64 = 3_010_299;
    let bits = value.to_bits() as i64;
    if bits == 0 {
        result[..3].copy_from_slice(b"0.0");
        return 3;
    }
    if bits == i64::MIN {
        result[..4].copy_from_slice(b"-0.0");
        return 4;
    }
    let ieee_exponent = (unsigned_shift(bits, MANTISSA_BITS) & EXPONENT_MASK) as i32;
    let ieee_mantissa = bits & MANTISSA_MASK;
    let (mut e2, m2) = if ieee_exponent == 0 {
        (1 - EXPONENT_BIAS - MANTISSA_BITS, ieee_mantissa)
    } else {
        (
            ieee_exponent - EXPONENT_BIAS - MANTISSA_BITS,
            ieee_mantissa | (1 << MANTISSA_BITS),
        )
    };
    let sign = bits < 0;
    let even = (m2 & 1) == 0;
    let mv = 4 * m2;
    let mp = 4 * m2 + 2;
    let mm_shift = i64::from(m2 != (1 << MANTISSA_BITS) || ieee_exponent <= 1);
    let mm = 4 * m2 - 1 - mm_shift;
    e2 -= 2;

    let tables = &*DOUBLE_TABLES;
    let (mut dv, mut dp, mut dm);
    let e10;
    let mut dm_is_trailing_zeros = false;
    let mut dv_is_trailing_zeros = false;
    if e2 >= 0 {
        let q = 0.max((i64::from(e2) * LOG10_2_NUMERATOR / 10_000_000) as i32 - 1);
        let k = 122 + pow5_bits(i64::from(q)) as i32 - 1;
        let i = -e2 + q + k;
        let actual_shift = i - 3 * 31 - 21;
        assert!(actual_shift >= 0, "{actual_shift}");
        let ints = &tables.pow5_inv_split[q as usize];
        dv = mul_shift(mv, ints, actual_shift);
        dp = mul_shift(mp, ints, actual_shift);
        dm = mul_shift(mm, ints, actual_shift);
        e10 = q;
        if q <= 21 {
            if mv % 5 == 0 {
                dv_is_trailing_zeros = pow5_factor(mv) >= q;
            } else if even {
                dm_is_trailing_zeros = pow5_factor(mm) >= q;
            } else if pow5_factor(mp) >= q {
                dp -= 1;
            }
        }
    } else {
        let q = 0.max((i64::from(-e2) * LOG10_5_NUMERATOR / 10_000_000) as i32 - 1);
        let i = -e2 - q;
        let k = pow5_bits(i64::from(i)) as i32 - 121;
        let j = q - k;
        let actual_shift = j - 3 * 31 - 21;
        assert!(actual_shift >= 0, "{actual_shift}");
        let ints = &tables.pow5_split[i as usize];
        dv = mul_shift(mv, ints, actual_shift);
        dp = mul_shift(mp, ints, actual_shift);
        dm = mul_shift(mm, ints, actual_shift);
        e10 = q + e2;
        if q <= 1 {
            dv_is_trailing_zeros = true;
            if even {
                dm_is_trailing_zeros = mm_shift == 1;
            } else {
                dp -= 1;
            }
        } else if q < 63 {
            dv_is_trailing_zeros = (mv & ((1i64 << (q - 1)) - 1)) == 0;
        }
    }

    let vp_length = decimal_length(dp);
    let mut exp = e10 + vp_length - 1;
    let scientific = !((-3..7).contains(&exp));
    let mut removed = 0;
    let mut last_removed_digit = 0;
    let output;
    if dm_is_trailing_zeros || dv_is_trailing_zeros {
        while dp / 10 > dm / 10 {
            if dp < 100 && scientific {
                break;
            }
            dm_is_trailing_zeros &= dm % 10 == 0;
            dv_is_trailing_zeros &= last_removed_digit == 0;
            last_removed_digit = (dv % 10) as i32;
            dp /= 10;
            dv /= 10;
            dm /= 10;
            removed += 1;
        }
        if dm_is_trailing_zeros && even {
            while dm % 10 == 0 {
                if dp < 100 && scientific {
                    break;
                }
                dv_is_trailing_zeros &= last_removed_digit == 0;
                last_removed_digit = (dv % 10) as i32;
                dp /= 10;
                dv /= 10;
                dm /= 10;
                removed += 1;
            }
        }
        if dv_is_trailing_zeros && last_removed_digit == 5 && dv % 2 == 0 {
            last_removed_digit = 4;
        }
        output = dv
            + i64::from((dv == dm && !(dm_is_trailing_zeros && even)) || last_removed_digit >= 5);
    } else {
        while dp / 10 > dm / 10 {
            if dp < 100 && scientific {
                break;
            }
            last_removed_digit = (dv % 10) as i32;
            dp /= 10;
            dv /= 10;
            dm /= 10;
            removed += 1;
        }
        output = dv + i64::from(dv == dm || last_removed_digit >= 5);
    }
    let output_length = vp_length - removed;
    write_ryu_digits(sign, output, output_length, &mut exp, scientific, result)
}

/// Step 5 shared by both widths: `Double.toString` layout for the computed digits.
fn write_ryu_digits(
    sign: bool,
    mut output: i64,
    length: i32,
    exp: &mut i32,
    scientific: bool,
    result: &mut [u8; 32],
) -> usize {
    let mut index = 0usize;
    if sign {
        result[index] = b'-';
        index += 1;
    }
    let length = length as usize;
    if scientific {
        for i in 0..length - 1 {
            result[index + length - i] = b'0' + (output % 10) as u8;
            output /= 10;
        }
        result[index] = b'0' + (output % 10) as u8;
        result[index + 1] = b'.';
        index += length + 1;
        if length == 1 {
            result[index] = b'0';
            index += 1;
        }
        result[index] = b'E';
        index += 1;
        if *exp < 0 {
            result[index] = b'-';
            index += 1;
            *exp = -*exp;
        }
        if *exp >= 100 {
            result[index] = b'0' + (*exp / 100) as u8;
            index += 1;
            *exp %= 100;
            result[index] = b'0' + (*exp / 10) as u8;
            index += 1;
        } else if *exp >= 10 {
            result[index] = b'0' + (*exp / 10) as u8;
            index += 1;
        }
        result[index] = b'0' + (*exp % 10) as u8;
        index + 1
    } else if *exp < 0 {
        result[index] = b'0';
        result[index + 1] = b'.';
        index += 2;
        let mut zeros = -1;
        while zeros > *exp {
            result[index] = b'0';
            index += 1;
            zeros -= 1;
        }
        let current = index;
        for i in 0..length {
            result[current + length - i - 1] = b'0' + (output % 10) as u8;
            output /= 10;
            index += 1;
        }
        index
    } else if *exp as usize + 1 >= length {
        for i in 0..length {
            result[index + length - i - 1] = b'0' + (output % 10) as u8;
            output /= 10;
        }
        index += length;
        for _ in length..(*exp as usize + 1) {
            result[index] = b'0';
            index += 1;
        }
        result[index] = b'.';
        result[index + 1] = b'0';
        index + 2
    } else {
        let mut current = index + 1;
        for i in 0..length {
            if (length - i - 1) as i32 == *exp {
                result[current + length - i - 1] = b'.';
                current -= 1;
            }
            result[current + length - i - 1] = b'0' + (output % 10) as u8;
            output /= 10;
        }
        index + length + 1
    }
}

const FLOAT_POW5_SPLIT: [[i32; 2]; 47] = [
    [536870912, 0],
    [671088640, 0],
    [838860800, 0],
    [1048576000, 0],
    [655360000, 0],
    [819200000, 0],
    [1024000000, 0],
    [640000000, 0],
    [800000000, 0],
    [1000000000, 0],
    [625000000, 0],
    [781250000, 0],
    [976562500, 0],
    [610351562, 1073741824],
    [762939453, 268435456],
    [953674316, 872415232],
    [596046447, 1619001344],
    [745058059, 1486880768],
    [931322574, 1321730048],
    [582076609, 289210368],
    [727595761, 898383872],
    [909494701, 1659850752],
    [568434188, 1305842176],
    [710542735, 1632302720],
    [888178419, 1503507488],
    [555111512, 671256724],
    [693889390, 839070905],
    [867361737, 2122580455],
    [542101086, 521306416],
    [677626357, 1725374844],
    [847032947, 546105819],
    [1058791184, 145761362],
    [661744490, 91100851],
    [827180612, 1187617888],
    [1033975765, 1484522360],
    [646234853, 1196261931],
    [807793566, 2032198326],
    [1009741958, 1466506084],
    [631088724, 379695390],
    [788860905, 474619238],
    [986076131, 1130144959],
    [616297582, 437905143],
    [770371977, 1621123253],
    [962964972, 415791331],
    [601853107, 1333611405],
    [752316384, 1130143345],
    [940395480, 1412679181],
];

const FLOAT_POW5_INV_SPLIT: [[i32; 2]; 31] = [
    [268435456, 1],
    [214748364, 1717986919],
    [171798691, 1803886265],
    [137438953, 1013612282],
    [219902325, 1192282922],
    [175921860, 953826338],
    [140737488, 763061070],
    [225179981, 791400982],
    [180143985, 203624056],
    [144115188, 162899245],
    [230584300, 1978625710],
    [184467440, 1582900568],
    [147573952, 1266320455],
    [236118324, 308125809],
    [188894659, 675997377],
    [151115727, 970294631],
    [241785163, 1981968139],
    [193428131, 297084323],
    [154742504, 1955654377],
    [247588007, 1840556814],
    [198070406, 613451992],
    [158456325, 61264864],
    [253530120, 98023782],
    [202824096, 78419026],
    [162259276, 1780722139],
    [259614842, 1990161963],
    [207691874, 733136111],
    [166153499, 1016005619],
    [265845599, 337118801],
    [212676479, 699191770],
    [170141183, 988850146],
];

fn float_mul_shift(m: i32, split: &[i32; 2], shift: i32) -> i64 {
    let m = i64::from(m);
    (m.wrapping_mul(i64::from(split[0])) + (m.wrapping_mul(i64::from(split[1])) >> 31))
        >> (shift & 63)
}

fn int_pow5_factor(mut value: i32) -> i32 {
    let mut count = 0;
    while value > 0 {
        if value % 5 != 0 {
            break;
        }
        value /= 5;
        count += 1;
    }
    count
}

/// Writes the fastjson `RyuFloat` spelling of a finite value; returns the byte length.
fn ryu_float(value: f32, result: &mut [u8; 32]) -> usize {
    const MANTISSA_MASK: i32 = (1 << 23) - 1;
    const EXPONENT_MASK: i32 = 255;
    const EXPONENT_BIAS: i32 = 127;
    const LOG10_2_NUMERATOR: i64 = 3_010_299;
    const LOG10_5_DENOMINATOR: i64 = 10_000_000;
    const LOG10_5_NUMERATOR: i64 = 6_989_700;
    let bits = value.to_bits() as i32;
    if bits == 0 {
        result[..3].copy_from_slice(b"0.0");
        return 3;
    }
    if bits == i32::MIN {
        result[..4].copy_from_slice(b"-0.0");
        return 4;
    }
    let ieee_exponent = (bits >> 23) & EXPONENT_MASK;
    let ieee_mantissa = bits & MANTISSA_MASK;
    let (mut e2, m2) = if ieee_exponent == 0 {
        (1 - EXPONENT_BIAS - 23, ieee_mantissa)
    } else {
        (
            ieee_exponent - EXPONENT_BIAS - 23,
            ieee_mantissa | (1 << 23),
        )
    };
    let sign = bits < 0;
    let even = (m2 & 1) == 0;
    let mv = 4 * m2;
    let mp = 4 * m2 + 2;
    let mm = 4 * m2
        - if m2 != (1 << 23) || ieee_exponent <= 1 {
            2
        } else {
            1
        };
    e2 -= 2;

    let (mut dp, mut dv, mut dm);
    let e10;
    let dp_is_trailing_zeros;
    let dv_is_trailing_zeros;
    let mut dm_is_trailing_zeros;
    let mut last_removed_digit = 0;
    if e2 >= 0 {
        let q = (i64::from(e2) * LOG10_2_NUMERATOR / 10_000_000) as i32;
        let k = 59 + pow5_bits(i64::from(q)) as i32 - 1;
        let i = -e2 + q + k;
        let split = &FLOAT_POW5_INV_SPLIT[q as usize];
        dv = float_mul_shift(mv, split, i - 31) as i32;
        dp = float_mul_shift(mp, split, i - 31) as i32;
        dm = float_mul_shift(mm, split, i - 31) as i32;
        if q != 0 && (dp - 1) / 10 <= dm / 10 {
            let e = q - 1;
            let l = 59 + pow5_bits(i64::from(e)) as i32 - 1;
            let ii = -e2 + q - 1 + l;
            let removed = float_mul_shift(mv, &FLOAT_POW5_INV_SPLIT[(q - 1) as usize], ii - 31);
            last_removed_digit = (removed % 10) as i32;
        }
        e10 = q;
        dp_is_trailing_zeros = int_pow5_factor(mp) >= q;
        dv_is_trailing_zeros = int_pow5_factor(mv) >= q;
        dm_is_trailing_zeros = int_pow5_factor(mm) >= q;
    } else {
        let q = (i64::from(-e2) * LOG10_5_NUMERATOR / LOG10_5_DENOMINATOR) as i32;
        let i = -e2 - q;
        let k = pow5_bits(i64::from(i)) as i32 - 61;
        let j = q - k;
        let split = &FLOAT_POW5_SPLIT[i as usize];
        dv = float_mul_shift(mv, split, j - 31) as i32;
        dp = float_mul_shift(mp, split, j - 31) as i32;
        dm = float_mul_shift(mm, split, j - 31) as i32;
        if q != 0 && (dp - 1) / 10 <= dm / 10 {
            let e = i + 1;
            let j = q - 1 - (pow5_bits(i64::from(e)) as i32 - 61);
            let removed = float_mul_shift(mv, &FLOAT_POW5_SPLIT[(i + 1) as usize], j - 31);
            last_removed_digit = (removed % 10) as i32;
        }
        e10 = q + e2;
        dp_is_trailing_zeros = 1 >= q;
        dv_is_trailing_zeros =
            q < 23 && (mv & 1i32.wrapping_shl((q - 1) as u32).wrapping_sub(1)) == 0;
        dm_is_trailing_zeros = (if mm % 2 == 1 { 0 } else { 1 }) >= q;
    }

    let mut dp_length = 10;
    let mut factor = 1_000_000_000;
    while dp_length > 0 {
        if dp >= factor {
            break;
        }
        factor /= 10;
        dp_length -= 1;
    }
    let mut exp = e10 + dp_length - 1;
    let scientific = !((-3..7).contains(&exp));
    let mut removed = 0;
    if dp_is_trailing_zeros && !even {
        dp -= 1;
    }
    while dp / 10 > dm / 10 {
        if dp < 100 && scientific {
            break;
        }
        dm_is_trailing_zeros &= dm % 10 == 0;
        dp /= 10;
        last_removed_digit = dv % 10;
        dv /= 10;
        dm /= 10;
        removed += 1;
    }
    if dm_is_trailing_zeros && even {
        while dm % 10 == 0 {
            if dp < 100 && scientific {
                break;
            }
            dp /= 10;
            last_removed_digit = dv % 10;
            dv /= 10;
            dm /= 10;
            removed += 1;
        }
    }
    if dv_is_trailing_zeros && last_removed_digit == 5 && dv % 2 == 0 {
        last_removed_digit = 4;
    }
    let output =
        dv + i32::from((dv == dm && !(dm_is_trailing_zeros && even)) || last_removed_digit >= 5);
    let output_length = dp_length - removed;
    write_ryu_digits(
        sign,
        i64::from(output),
        output_length,
        &mut exp,
        scientific,
        result,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(text: &str) -> String {
        let mut output = Vec::new();
        write_string(text.as_bytes(), &mut output);
        String::from_utf8(output).unwrap()
    }

    fn double(value: f64) -> String {
        let mut output = Vec::new();
        write_double(value, &mut output);
        String::from_utf8(output).unwrap()
    }

    fn float(value: f32) -> String {
        let mut output = Vec::new();
        write_float(value, &mut output);
        String::from_utf8(output).unwrap()
    }

    #[test]
    fn strings_use_fastjson_default_escapes() {
        assert_eq!(string("plain/text"), "\"plain/text\"");
        assert_eq!(string("\u{8}\t\n\u{c}\r\"\\"), r#""\b\t\n\f\r\"\\""#);
        assert_eq!(string("\u{0}\u{b}\u{1f}"), r#""\u0000\u000B\u001F""#);
        assert_eq!(
            string("\u{7f}\u{80}\u{9f}\u{a0}"),
            "\"\\u007F\\u0080\\u009F\u{a0}\""
        );
        assert_eq!(
            string("a\u{2028}b\u{2029}\u{2027}"),
            "\"a\\u2028b\\u2029\u{2027}\""
        );
        assert_eq!(string("é😀"), "\"é😀\"");
    }

    #[test]
    fn doubles_follow_fastjson_ryu_layout() {
        assert_eq!(double(0.0), "0.0");
        assert_eq!(double(-0.0), "-0.0");
        assert_eq!(double(1.0), "1.0");
        assert_eq!(double(0.1), "0.1");
        assert_eq!(double(1e-3), "0.001");
        assert_eq!(double(1e-4), "1.0E-4");
        assert_eq!(double(1e7), "1.0E7");
        assert_eq!(double(1234567.0), "1234567.0");
        assert_eq!(double(1e23), "1.0E23");
        assert_eq!(double(f64::MAX), "1.7976931348623157E308");
        assert_eq!(double(f64::NAN), "null");
        assert_eq!(double(f64::NEG_INFINITY), "null");
    }

    #[test]
    fn floats_follow_fastjson_ryu_layout() {
        assert_eq!(float(0.0), "0.0");
        assert_eq!(float(-0.0), "-0.0");
        assert_eq!(float(1.0), "1.0");
        assert_eq!(float(0.1), "0.1");
        assert_eq!(float(1e7), "1.0E7");
        assert_eq!(float(f32::MAX), "3.4028235E38");
        assert_eq!(float(f32::INFINITY), "null");
    }
}
