use super::*;

pub(super) fn equal(left: &ScalarValue, right: &ScalarValue) -> Option<bool> {
    nested_equal(left, right, false)
}

pub(super) fn boxed_equal(left: &ScalarValue, right: &ScalarValue) -> Option<bool> {
    nested_equal(left, right, true)
}

fn nested_equal(left: &ScalarValue, right: &ScalarValue, boxed: bool) -> Option<bool> {
    macro_rules! arrays {
        ($left:expr, $right:expr) => {{
            if boxed {
                let left = $left.data_type();
                let right = $right.data_type();
                if !same_value_type(left, right)
                    || streamfusion_bridge::timestamp::is_component_timestamp(left)
                        != streamfusion_bridge::timestamp::is_component_timestamp(right)
                {
                    return Some(false);
                }
            } else {
                if $left.data_type() != $right.data_type() {
                    return Some(false);
                }
                if contains_floating($left.data_type()) {
                    return None;
                }
            }
            value_equal($left.as_ref(), 0, $right.as_ref(), 0)
        }};
    }
    match (left, right) {
        (ScalarValue::Struct(a), ScalarValue::Struct(b)) => arrays!(a, b),
        (ScalarValue::List(a), ScalarValue::List(b)) => arrays!(a, b),
        (ScalarValue::LargeList(a), ScalarValue::LargeList(b)) => arrays!(a, b),
        (ScalarValue::FixedSizeList(a), ScalarValue::FixedSizeList(b)) => arrays!(a, b),
        (ScalarValue::Map(a), ScalarValue::Map(b)) => arrays!(a, b),
        _ => None,
    }
}

fn same_value_type(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b))
        | (DataType::Map(a, _), DataType::Map(b, _)) => {
            same_value_type(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, n), DataType::FixedSizeList(b, m)) => {
            n == m && same_value_type(a.data_type(), b.data_type())
        }
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| same_value_type(a.data_type(), b.data_type()))
        }
        _ => left == right,
    }
}

fn value_equal(left: &dyn Array, l: usize, right: &dyn Array, r: usize) -> Option<bool> {
    if left.is_null(l) || right.is_null(r) {
        return Some(left.is_null(l) == right.is_null(r));
    }
    macro_rules! values {
        ($array:ident) => {{
            let left = left
                .as_any()
                .downcast_ref::<arrow::array::$array>()
                .unwrap();
            let right = right
                .as_any()
                .downcast_ref::<arrow::array::$array>()
                .unwrap();
            Some(left.value(l) == right.value(r))
        }};
    }
    macro_rules! lists {
        ($array:ident) => {{
            let left = left
                .as_any()
                .downcast_ref::<arrow::array::$array>()
                .unwrap();
            let right = right
                .as_any()
                .downcast_ref::<arrow::array::$array>()
                .unwrap();
            let ls = left.value_offsets()[l] as usize;
            let le = left.value_offsets()[l + 1] as usize;
            let rs = right.value_offsets()[r] as usize;
            let re = right.value_offsets()[r + 1] as usize;
            range_equal(
                left.values().as_ref(),
                ls,
                le,
                right.values().as_ref(),
                rs,
                re,
            )
        }};
    }
    match left.data_type() {
        DataType::Boolean => values!(BooleanArray),
        DataType::Int8 => values!(Int8Array),
        DataType::Int16 => values!(Int16Array),
        DataType::Int32 => values!(Int32Array),
        DataType::Int64 => values!(Int64Array),
        DataType::UInt8 => values!(UInt8Array),
        DataType::UInt16 => values!(UInt16Array),
        DataType::UInt32 => values!(UInt32Array),
        DataType::UInt64 => values!(UInt64Array),
        DataType::Float32 => {
            let left = left.as_primitive::<Float32Type>().value(l);
            let right = right.as_primitive::<Float32Type>().value(r);
            Some(canonical_f32(left).to_bits() == canonical_f32(right).to_bits())
        }
        DataType::Float64 => {
            let left = left.as_primitive::<Float64Type>().value(l);
            let right = right.as_primitive::<Float64Type>().value(r);
            Some(canonical_f64(left).to_bits() == canonical_f64(right).to_bits())
        }
        DataType::Date32 => values!(Date32Array),
        DataType::Date64 => values!(Date64Array),
        DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
            values!(Time32MillisecondArray)
        }
        DataType::Time32(arrow::datatypes::TimeUnit::Second) => values!(Time32SecondArray),
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            values!(Time64MicrosecondArray)
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => values!(Time64NanosecondArray),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => values!(TimestampSecondArray),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            values!(TimestampMillisecondArray)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            values!(TimestampMicrosecondArray)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            values!(TimestampNanosecondArray)
        }
        DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
            values!(IntervalYearMonthArray)
        }
        DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => {
            values!(IntervalDayTimeArray)
        }
        DataType::Decimal128(_, _) => values!(Decimal128Array),
        DataType::Utf8 => values!(StringArray),
        DataType::LargeUtf8 => values!(LargeStringArray),
        DataType::Binary => values!(BinaryArray),
        DataType::LargeBinary => values!(LargeBinaryArray),
        DataType::FixedSizeBinary(_) => values!(FixedSizeBinaryArray),
        DataType::List(_) => lists!(ListArray),
        DataType::LargeList(_) => lists!(LargeListArray),
        DataType::FixedSizeList(_, width) => {
            let left = left
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()
                .unwrap();
            let right = right
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()
                .unwrap();
            let ls = l * *width as usize;
            let rs = r * *width as usize;
            range_equal(
                left.values().as_ref(),
                ls,
                ls + *width as usize,
                right.values().as_ref(),
                rs,
                rs + *width as usize,
            )
        }
        DataType::Struct(_) => {
            let left = left
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            let right = right
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            for (left, right) in left.columns().iter().zip(right.columns()) {
                if !value_equal(left.as_ref(), l, right.as_ref(), r)? {
                    return Some(false);
                }
            }
            Some(true)
        }
        DataType::Map(_, _) => {
            let left = left
                .as_any()
                .downcast_ref::<arrow::array::MapArray>()
                .unwrap();
            let right = right
                .as_any()
                .downcast_ref::<arrow::array::MapArray>()
                .unwrap();
            range_equal(
                left.entries(),
                left.value_offsets()[l] as usize,
                left.value_offsets()[l + 1] as usize,
                right.entries(),
                right.value_offsets()[r] as usize,
                right.value_offsets()[r + 1] as usize,
            )
        }
        _ => None,
    }
}

fn range_equal(
    left: &dyn Array,
    ls: usize,
    le: usize,
    right: &dyn Array,
    rs: usize,
    re: usize,
) -> Option<bool> {
    if le - ls != re - rs {
        return Some(false);
    }
    for offset in 0..le - ls {
        if !value_equal(left, ls + offset, right, rs + offset)? {
            return Some(false);
        }
    }
    Some(true)
}
