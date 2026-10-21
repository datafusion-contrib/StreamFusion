use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use std::hash::{Hash, Hasher};

/// Child field names, nullability and metadata do not participate in value membership.
pub(super) fn value<H: Hasher>(array: &dyn Array, row: usize, state: &mut H) {
    std::mem::discriminant(array.data_type()).hash(state);
    let null = array.is_null(row);
    null.hash(state);
    if null {
        return;
    }
    macro_rules! primitive {
        ($ty:ident) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .value(row)
                .hash(state)
        };
    }
    macro_rules! list {
        ($ty:ident) => {{
            let array = array.as_any().downcast_ref::<$ty>().unwrap();
            range(
                array.values().as_ref(),
                array.value_offsets()[row] as usize,
                array.value_offsets()[row + 1] as usize,
                state,
            );
        }};
    }
    match array.data_type() {
        DataType::Boolean => primitive!(BooleanArray),
        DataType::Int8 => primitive!(Int8Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::UInt8 => primitive!(UInt8Array),
        DataType::UInt16 => primitive!(UInt16Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::UInt64 => primitive!(UInt64Array),
        DataType::Float32 => crate::flink_float::canonical_f32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row),
        )
        .to_bits()
        .hash(state),
        DataType::Float64 => crate::flink_float::canonical_f64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        )
        .to_bits()
        .hash(state),
        DataType::Decimal128(_, _) => primitive!(Decimal128Array),
        DataType::Utf8 => primitive!(StringArray),
        DataType::LargeUtf8 => primitive!(LargeStringArray),
        DataType::Binary => primitive!(BinaryArray),
        DataType::LargeBinary => primitive!(LargeBinaryArray),
        DataType::FixedSizeBinary(_) => primitive!(FixedSizeBinaryArray),
        DataType::Date32 => primitive!(Date32Array),
        DataType::Date64 => primitive!(Date64Array),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondArray),
        DataType::List(_) => list!(ListArray),
        DataType::LargeList(_) => list!(LargeListArray),
        DataType::FixedSizeList(_, width) => {
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            range(
                array.values().as_ref(),
                row * *width as usize,
                (row + 1) * *width as usize,
                state,
            );
        }
        DataType::Struct(_) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array.num_columns().hash(state);
            for column in array.columns() {
                value(column.as_ref(), row, state);
            }
        }
        DataType::Map(_, _) => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            range(
                array.entries(),
                array.value_offsets()[row] as usize,
                array.value_offsets()[row + 1] as usize,
                state,
            );
        }
        // Unhandled leaf encodings retain a type bucket; equality remains authoritative.
        _ => {}
    }
}

fn range<H: Hasher>(array: &dyn Array, start: usize, end: usize, state: &mut H) {
    (end - start).hash(state);
    for row in start..end {
        value(array, row, state);
    }
}
