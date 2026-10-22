use crate::*;

// Positional multipliers from Flink's generated RowData hash, observable in heap MAP matching.
const SALT: [i32; 32] = [
    73, 79, 97, 113, 131, 197, 199, 311, 337, 373, 719, 733, 919, 971, 991, 1193, 1931, 3119, 3779,
    7793, 7937, 9311, 9377, 11939, 19391, 19937, 37199, 39119, 71993, 91193, 93719, 93911,
];

pub(super) fn row(fields: &[Arc<Field>], values: &[ScalarValue]) -> i32 {
    fields
        .iter()
        .zip(values)
        .enumerate()
        .fold(0i32, |hash, (i, (field, value))| {
            hash.wrapping_mul(SALT[i & 31])
                .wrapping_add(value_hash(field, value))
        })
}

pub(super) fn bytes(bytes: &[u8]) -> i32 {
    let mix = |hash: u32, word: u32| {
        let word = word
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593);
        (hash ^ word)
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe6546b64)
    };
    let mut hash = 42u32;
    let mut words = bytes.chunks_exact(4);
    for word in &mut words {
        hash = mix(hash, u32::from_ne_bytes(word.try_into().unwrap()));
    }
    for &byte in words.remainder() {
        hash = mix(hash, (byte as i8 as i32) as u32);
    }
    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    (hash ^ (hash >> 16)) as i32
}

fn long(value: i64) -> i32 {
    (value ^ (value >> 32)) as i32
}
fn marked(field: &Field, name: &str) -> bool {
    field.metadata().get(name).is_some_and(|s| s == "1")
}
fn scalar(array: &ArrayRef, i: usize) -> ScalarValue {
    ScalarValue::try_from_array(array, i).expect("materializer hash value")
}

fn value_hash(field: &Field, value: &ScalarValue) -> i32 {
    use ScalarValue::*;
    if value.is_null() {
        return 0;
    }
    if streamfusion_bridge::timestamp::is_timestamp(field.data_type()) {
        let array = value.to_array().expect("timestamp scalar");
        let time = streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
            .expect("timestamp hash")
            .value(0)
            .expect("timestamp in millisecond range");
        return long(time.millis())
            .wrapping_mul(31)
            .wrapping_add(time.nano_of_milli() as i32);
    }
    match value {
        Boolean(Some(v)) => {
            if *v {
                1231
            } else {
                1237
            }
        }
        Int8(Some(v)) => *v as i32,
        Int16(Some(v)) => *v as i32,
        Int32(Some(v))
        | Date32(Some(v))
        | Time32Millisecond(Some(v))
        | IntervalYearMonth(Some(v)) => *v,
        Int64(Some(v)) | DurationMillisecond(Some(v)) => long(*v),
        Time32Second(Some(v)) => v.wrapping_mul(1000),
        Time64Microsecond(Some(v)) => v.div_euclid(1000) as i32,
        Time64Nanosecond(Some(v)) => v.div_euclid(1_000_000) as i32,
        Float32(Some(v)) => crate::flink_float::canonical_f32(*v).to_bits() as i32,
        Float64(Some(v)) => long(crate::flink_float::canonical_f64(*v).to_bits() as i64),
        Utf8(Some(v)) | LargeUtf8(Some(v)) | Utf8View(Some(v)) => bytes(v.as_bytes()),
        Binary(Some(v))
        | LargeBinary(Some(v))
        | BinaryView(Some(v))
        | FixedSizeBinary(_, Some(v)) => bytes(v),
        Decimal128(Some(v), _, scale) => {
            let unsigned = v.unsigned_abs();
            let mut h = 0i32;
            for shift in (0..4).rev() {
                h = h
                    .wrapping_mul(31)
                    .wrapping_add((unsigned >> (32 * shift)) as u32 as i32);
            }
            if *v < 0 {
                h = h.wrapping_neg();
            }
            h.wrapping_mul(31).wrapping_add(*scale as i32)
        }
        Struct(a) => {
            if crate::flink_key::represented_string(a.data_type()) {
                return value_hash(&a.fields()[0], &scalar(a.column(0), 0));
            }
            if a.fields().len() == 2 && marked(&a.fields()[1], "streamfusion.array-element") {
                return value_hash(&a.fields()[0], &scalar(a.column(0), 0));
            }
            let kind = nested_row_kind_index(a.fields());
            let mut i = 0;
            let mut h = 0i32;
            for (index, f) in a.fields().iter().enumerate() {
                if Some(index) == kind || marked(f, "streamfusion.nested-row-binary") {
                    continue;
                }
                h = h
                    .wrapping_mul(SALT[i & 31])
                    .wrapping_add(value_hash(f, &scalar(a.column(index), 0)));
                i += 1;
            }
            h
        }
        List(a) => {
            let DataType::List(element) = field.data_type() else {
                unreachable!()
            };
            let values = a.value(0);
            if marked(field, "streamfusion.map-list") || marked(element, "streamfusion.map-list") {
                map(element, &values)
            } else {
                array(element, &values)
            }
        }
        LargeList(a) => {
            let DataType::LargeList(element) = field.data_type() else {
                unreachable!()
            };
            array(element, &a.value(0))
        }
        FixedSizeList(a) => {
            let DataType::FixedSizeList(element, _) = field.data_type() else {
                unreachable!()
            };
            array(element, &a.value(0))
        }
        Map(a) => {
            let DataType::Map(element, _) = field.data_type() else {
                unreachable!()
            };
            map(element, &(Arc::new(a.value(0)) as ArrayRef))
        }
        _ => panic!("unsupported materializer generated hash: {value:?}"),
    }
}

fn array(field: &Field, values: &ArrayRef) -> i32 {
    (0..values.len()).fold(1i32, |h, i| {
        h.wrapping_mul(31)
            .wrapping_add(value_hash(field, &scalar(values, i)))
    })
}

fn map(field: &Field, values: &ArrayRef) -> i32 {
    let values = values
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map entries");
    let DataType::Struct(fields) = field.data_type() else {
        unreachable!()
    };
    (0..values.len()).fold(0i32, |h, i| {
        h.wrapping_add(
            value_hash(&fields[0], &scalar(values.column(0), i))
                ^ value_hash(&fields[1], &scalar(values.column(1), i)),
        )
    })
}
