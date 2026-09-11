use super::*;

fn bodies(docs: Vec<Option<&[u8]>>) -> RecordBatch {
    let column: ArrayRef = Arc::new(BinaryArray::from(docs));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::Binary,
            true,
        )])),
        vec![column],
    )
    .unwrap()
}

// `raw` (format 3): the body bytes pass through as the single column, cast to the declared type.
#[test]
fn raw_decode_passes_bytes_through() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        true,
    )]));
    let body = bodies(vec![Some(b"hello"), Some(b"world")]);
    let out = new_decoder(FORMAT_RAW, schema, "", "", 0, false, "").decode(&body);
    assert_eq!(out.num_rows(), 2);
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((col.value(0), col.value(1)), ("hello", "world"));
}

fn raw_decode(data_type: DataType, docs: Vec<Option<&[u8]>>, options: &str) -> RecordBatch {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("payload", data_type, true)]));
    new_decoder(FORMAT_RAW, schema, "", "", 0, false, options).decode(&bodies(docs))
}

// Raw fixed-width numerics read the exact-length message with the configured endianness
// (big-endian is Flink's default), and a null body stays a null field.
#[test]
fn raw_decode_reads_fixed_width_values_with_the_configured_endianness() {
    use arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array,
    };
    let out = raw_decode(
        DataType::Int32,
        vec![Some(&0x12345678i32.to_be_bytes()), None],
        "",
    );
    let col = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(col.value(0), 0x12345678);
    assert!(col.is_null(1));

    let little = "raw.endianness=little-endian\n";
    let out = raw_decode(
        DataType::Int32,
        vec![Some(&0x12345678i32.to_le_bytes())],
        little,
    );
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        0x12345678
    );

    let out = raw_decode(DataType::Int16, vec![Some(&(-2i16).to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        -2
    );
    let out = raw_decode(DataType::Int16, vec![Some(&(-2i16).to_le_bytes())], little);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        -2
    );

    let out = raw_decode(DataType::Int64, vec![Some(&i64::MIN.to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );
    let out = raw_decode(DataType::Int64, vec![Some(&i64::MIN.to_le_bytes())], little);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );

    let out = raw_decode(DataType::Float32, vec![Some(&1.5f32.to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        1.5
    );
    let out = raw_decode(DataType::Float32, vec![Some(&1.5f32.to_le_bytes())], little);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        1.5
    );

    let out = raw_decode(DataType::Float64, vec![Some(&(-2.25f64).to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        -2.25
    );
    let out = raw_decode(
        DataType::Float64,
        vec![Some(&(-2.25f64).to_le_bytes())],
        little,
    );
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        -2.25
    );

    // The one-byte types ignore endianness: TINYINT is the signed byte, BOOLEAN is `byte != 0`.
    let out = raw_decode(DataType::Int8, vec![Some(&[0xff]), Some(&[0x7f])], little);
    let col = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!((col.value(0), col.value(1)), (-1, 127));
    let out = raw_decode(
        DataType::Boolean,
        vec![Some(&[0]), Some(&[1]), Some(&[2]), None],
        "",
    );
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        (col.value(0), col.value(1), col.value(2)),
        (false, true, true)
    );
    assert!(col.is_null(3));
}

// A wrong-length message fails the decode with Flink's own DeserializationException text
// (raw has no ignore-parse-errors; the job fails exactly as Flink's does).
#[test]
#[should_panic(expected = "Size of data received for deserializing INT type is not 4.")]
fn raw_decode_rejects_a_wrong_length_message_like_flink() {
    raw_decode(DataType::Int32, vec![Some(&[1, 2, 3])], "");
}

#[test]
#[should_panic(expected = "Size of data received for deserializing BOOLEAN type is not 1.")]
fn raw_decode_rejects_an_empty_boolean_message_like_flink() {
    raw_decode(DataType::Boolean, vec![Some(&[])], "");
}

// Invalid UTF-8 under a STRING column fails the decode loudly (the documented divergence: Flink
// smuggles the bytes through StringData unvalidated, Arrow strings cannot) — never a silent NULL.
#[test]
#[should_panic(expected = "raw format STRING message is not valid UTF-8")]
fn raw_decode_rejects_invalid_utf8_strings() {
    raw_decode(
        DataType::Utf8,
        vec![Some(b"ok"), Some(&[0xff, 0xfe, 0x68, 0x69])],
        "",
    );
}

// Binary columns take the message verbatim at any length (including empty), nulls staying null.
#[test]
fn raw_decode_passes_binary_bodies_verbatim() {
    use arrow::array::Array;
    let out = raw_decode(
        DataType::Binary,
        vec![Some(&[0xde, 0xad, 0xbe, 0xef]), Some(&[]), None],
        "",
    );
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), &[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(col.value(1), b"");
    assert!(col.is_null(2));
}
