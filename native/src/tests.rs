use super::*;
use arrow::array::BinaryArray;

#[test]
fn max_rowtime_skips_nulls_and_floors_millis() {
    use arrow::array::TimestampNanosecondArray;
    let rowtime: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        Some(1_000_000_123i64), // 1.000000123s -> floors to 1000ms
        None,
        Some(999_999_999),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        )])),
        vec![rowtime],
    )
    .unwrap();
    assert_eq!(1000, max_rowtime_millis(&batch, 0));
}

#[test]
fn max_rowtime_floors_pre_epoch_and_signals_all_null() {
    use arrow::array::TimestampNanosecondArray;
    // -1ns is inside the millisecond before the epoch: Flink's TimestampData stores it as
    // millisecond -1 (floor), not 0 (truncation toward zero).
    let pre_epoch: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(-1i64)]));
    let all_null: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        true,
    )]));
    let pre = RecordBatch::try_new(schema.clone(), vec![pre_epoch]).unwrap();
    let none = RecordBatch::try_new(schema, vec![all_null]).unwrap();
    assert_eq!(-1, max_rowtime_millis(&pre, 0));
    assert_eq!(i64::MIN, max_rowtime_millis(&none, 0));
}

#[test]
fn max_rowtime_reads_epoch_millis_bigint_verbatim() {
    // A TO_TIMESTAMP_LTZ(col, 3) computed rowtime: the physical column already holds epoch millis.
    let millis: ArrayRef = Arc::new(Int64Array::from(vec![Some(90_000i64), None, Some(10_000)]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("dateTime", DataType::Int64, true)])),
        vec![millis],
    )
    .unwrap();
    assert_eq!(90_000, max_rowtime_millis(&batch, 0));
}

fn sample_batch() -> RecordBatch {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 6, 3, 9]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 0, 8, 2]));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        vec![a, b],
    )
    .unwrap()
}

fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch.column(column).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
}

fn json_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

fn bodies(docs: Vec<Option<&[u8]>>) -> RecordBatch {
    let column: ArrayRef = Arc::new(BinaryArray::from(docs));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
        vec![column],
    )
    .unwrap()
}

/// A hand-built `FileDescriptorSet` for `bench.Row { int64 id=1; string name=2; double score=3; }`
/// — what the JVM would serialize off the generated message class for Flink's `protobuf` format.
fn proto_descriptor_set() -> Vec<u8> {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let message = DescriptorProto {
        name: Some("Row".to_string()),
        field: vec![
            field("id", 1, Type::Int64),
            field("name", 2, Type::String),
            field("score", 3, Type::Double),
        ],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("bench.proto".to_string()),
        package: Some("bench".to_string()),
        message_type: vec![message],
        syntax: Some("proto3".to_string()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

// Each body is one bare protobuf message (no framing); ptars decodes the wire format straight into
// Arrow arrays, deriving the batch schema from the descriptor (columns named by proto field).
#[test]
fn protobuf_decode_emits_one_row_per_message() {
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_descriptor_set();
    let message = DescriptorPool::decode(descriptor.as_ref())
        .unwrap()
        .get_message_by_name("bench.Row")
        .unwrap();
    let encode = |id: i64, name: &str, score: f64| {
        let mut m = DynamicMessage::new(message.clone());
        m.set_field_by_name("id", Value::I64(id));
        m.set_field_by_name("name", Value::String(name.to_string()));
        m.set_field_by_name("score", Value::F64(score));
        m.encode_to_vec()
    };
    let row0 = encode(1, "a", 1.5);
    let row1 = encode(2, "b", 2.5);
    let body = bodies(vec![Some(row0.as_slice()), Some(row1.as_slice())]);

    let out = ProtobufDecoder::new(&descriptor, "bench.Row").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let id = out.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names =
        out.column_by_name("name").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
    let scores =
        out.column_by_name("score").unwrap().as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5]);
}

// Each body is one CSV record (no header); CSV decode (format 2) emits one typed row per record.
#[test]
fn csv_decode_emits_one_row_per_record() {
    let body = bodies(vec![Some(b"1,a,1.5"), Some(b"2,b,2.5")]);
    let out = MessageDecoder::new(2, json_schema(), "", "", 0, false, "").decode(&body);
    assert_eq!(out.num_rows(), 2);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
    let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5]);
}

// `raw` (format 3): the body bytes pass through as the single column, cast to the declared type.
#[test]
fn raw_decode_passes_bytes_through() {
    let schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("payload", DataType::Utf8, true)]));
    let body = bodies(vec![Some(b"hello"), Some(b"world")]);
    let out = MessageDecoder::new(3, schema, "", "", 0, false, "").decode(&body);
    assert_eq!(out.num_rows(), 2);
    let col = out.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!((col.value(0), col.value(1)), ("hello", "world"));
}

// Bare Avro (format 4): each body is a raw datum (no Confluent framing), decoded against the reader
// schema we register at synthetic id 0 (the decoder prepends the id-0 header internally).
#[test]
fn bare_avro_decode_emits_one_row_per_datum() {
    // Avro binary datum for record { long id; string name; double score }, no framing.
    fn zigzag_varint(n: i64) -> Vec<u8> {
        let mut zz = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let mut b = (zz & 0x7f) as u8;
            zz >>= 7;
            if zz != 0 {
                b |= 0x80;
            }
            out.push(b);
            if zz == 0 {
                break;
            }
        }
        out
    }
    fn datum(id: i64, name: &str, score: f64) -> Vec<u8> {
        let mut v = zigzag_varint(id);
        v.extend(zigzag_varint(name.len() as i64));
        v.extend_from_slice(name.as_bytes());
        v.extend_from_slice(&score.to_le_bytes());
        v
    }
    let reader_schema = r#"{"type":"record","name":"Row","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"score","type":"double"}]}"#;
    let m0 = datum(1, "a", 1.5);
    let m1 = datum(2, "b", 2.5);
    let body = bodies(vec![Some(m0.as_slice()), Some(m1.as_slice())]);

    let out = MessageDecoder::new(4, Arc::new(Schema::empty()), reader_schema, "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let id = out.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names =
        out.column_by_name("name").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
}

// Byte-exact decimal division/modulo: every expected value below was produced by running Java's
// own BigDecimal pipeline (divide with MathContext(38, HALF_UP), then setScale(s, HALF_UP), with
// DecimalData.fromBigDecimal's precision check) — the exact code Flink's runtime executes.
#[test]
fn decimal_divide_matches_bigdecimal() {
    fn div(a: i128, s1: i8, b: i128, s2: i8, p: u8, s: i8) -> Option<i128> {
        let (unscaled, scale) = quotient_38_digits(a, s1, b, s2);
        rescale_half_up(unscaled, scale, p, s)
    }
    // 7.00 / 3.00 → DECIMAL(23,13): the repeating quotient rounds at the declared scale.
    assert_eq!(div(700, 2, 300, 2, 23, 13), Some(23333333333333));
    // 2 / 3 → DECIMAL(38,6): the 38-significant-digit intermediate then rescales with HALF_UP.
    assert_eq!(div(2, 0, 3, 0, 38, 6), Some(666667));
    // Negative dividend: HALF_UP rounds away from zero.
    assert_eq!(div(-700, 2, 300, 2, 23, 13), Some(-23333333333333));
    assert_eq!(div(1, 0, 3, 0, 10, 2), Some(33));
    // 10.4 / 0.03 → 346.666667 (rounded up at the target scale).
    assert_eq!(div(104, 1, 3, 2, 12, 6), Some(346666667));
    // 99999999999999999999.5 / 0.1: an exact 21-digit quotient, rescaled to 22 digits — fits.
    assert_eq!(
        div(999999999999999999995, 1, 1, 1, 22, 1),
        Some(9999999999999999999950)
    );
    assert_eq!(div(0, 2, 525, 2, 23, 13), Some(0));
    // A quotient needing more digits than the declared precision reports NULL, like
    // DecimalData.fromBigDecimal.
    assert_eq!(div(123456789012345678901234567890123456, 6, 1, 6, 38, 6), None);
}

#[test]
fn decimal_mod_matches_bigdecimal() {
    fn modulo(a: i128, s1: i8, b: i128, s2: i8, p: u8, s: i8) -> Option<i128> {
        let (unscaled, scale) = remainder_exact(a, s1, b, s2);
        rescale_half_up(unscaled, scale, p, s)
    }
    // 7.5 % 2.1 = 1.2; the sign follows the dividend (Java remainder), the divisor's sign is
    // irrelevant.
    assert_eq!(modulo(75, 1, 21, 1, 12, 6), Some(1_200_000));
    assert_eq!(modulo(-75, 1, 21, 1, 12, 6), Some(-1_200_000));
    assert_eq!(modulo(75, 1, -21, 1, 12, 6), Some(1_200_000));
    // Mixed scales: 5.75 % 0.50 = 0.25.
    assert_eq!(modulo(575, 2, 50, 2, 12, 6), Some(250_000));
}

// Confluent Avro (format 1), registry-driven: the store starts empty, writer schemas arrive by id
// (as the JVM fetches them from the schema registry), and each message resolves against the reader
// schema. Covers the two things the single-schema path never exercised: a mid-batch schema-id
// switch (the decoder flushes internally; the flushes concatenate under the one reader shape) and
// a writer record named differently from the reader (matched through the alias the JVM patches in,
// mirroring Avro Java's lenient name check).
#[test]
fn confluent_avro_decodes_evolving_writer_schemas_against_reader() {
    fn zigzag_varint(n: i64) -> Vec<u8> {
        let mut zz = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let mut b = (zz & 0x7f) as u8;
            zz >>= 7;
            if zz != 0 {
                b |= 0x80;
            }
            out.push(b);
            if zz == 0 {
                break;
            }
        }
        out
    }
    fn string_field(s: &str) -> Vec<u8> {
        let mut v = zigzag_varint(s.len() as i64);
        v.extend_from_slice(s.as_bytes());
        v
    }
    fn framed(id: u32, datum: Vec<u8>) -> Vec<u8> {
        let mut v = vec![0x00];
        v.extend_from_slice(&id.to_be_bytes());
        v.extend(datum);
        v
    }
    let reader = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    // Writer 7: a producer-named record with an extra trailing field the reader drops; the JVM
    // patches in the reader's full name as an alias so arrow-avro's name check passes (Avro Java
    // skips that check entirely).
    let writer_v1 = r#"{"type":"record","name":"User","namespace":"com.example",
            "aliases":["org.apache.flink.avro.generated.record"],"fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"extra","type":"string"}]}"#;
    // Writer 9: evolved — fields reordered; resolution matches them by name.
    let writer_v2 = r#"{"type":"record","name":"UserV2","namespace":"com.example",
            "aliases":["org.apache.flink.avro.generated.record"],"fields":[
            {"name":"name","type":"string"},{"name":"id","type":"long"}]}"#;

    let mut decoder = MessageDecoder::new(1, Arc::new(Schema::empty()), "", reader, 0, false, "");
    decoder.register_writer_schema(7, writer_v1);
    decoder.register_writer_schema(9, writer_v2);

    let mut d1 = zigzag_varint(1);
    d1.extend(string_field("a"));
    d1.extend(string_field("dropped"));
    let mut d2 = string_field("b");
    d2.extend(zigzag_varint(2));
    let mut d3 = zigzag_varint(3);
    d3.extend(string_field("c"));
    d3.extend(string_field("dropped"));
    let (m1, m2, m3) = (framed(7, d1), framed(9, d2), framed(7, d3));
    let body = bodies(vec![Some(&m1), Some(&m2), Some(&m3)]);

    let out = decoder.decode(&body);

    assert_eq!(out.num_rows(), 3);
    let id = out.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 3]);
    let names = out
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((names.value(0), names.value(1), names.value(2)), ("a", "b", "c"));
}

// Debezium JSON (format 6): the `{before, after, op}` envelope fans out to a columnar changelog —
// c/r → one INSERT row from `after`, u → UPDATE_BEFORE (from `before`) + UPDATE_AFTER (from `after`),
// d → one DELETE row from `before` — with each row's `RowKind` on the trailing `$row_kind$` column.
#[test]
fn cdc_debezium_decode_emits_changelog() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c","ts_ms":7}"#;
    let update =
        br#"{"before":{"id":2,"name":"b","score":2.5},"after":{"id":2,"name":"b2","score":3.5},"op":"u"}"#;
    let delete = br#"{"before":{"id":3,"name":"c","score":4.5},"after":null,"op":"d"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

    let out = MessageDecoder::new(6, json_schema(), "", "", 0, false, "").decode(&body);

    // 1 (insert) + 2 (update) + 1 (delete) physical rows.
    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(
        (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "b2", "c"]
    );
    let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5, 3.5, 4.5]);
    // INSERT(0), UPDATE_BEFORE(1), UPDATE_AFTER(2), DELETE(3) — Flink's RowKind byte values.
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 1, 2, 3]);
    assert_eq!(out.schema().field(3).name(), ROW_KIND_COLUMN);
}

// A tombstone (null body) is dropped, leaving the valid records — matching Flink, which skips
// empty/null messages regardless of error handling.
#[test]
fn cdc_debezium_skips_tombstone() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"r"}"#;
    let body = bodies(vec![None, Some(insert.as_slice())]);

    let out = MessageDecoder::new(6, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 1);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0]); // "r" snapshot read → INSERT
}

// An unrecognized op fails the decode rather than silently dropping the row — Flink throws on it by
// default, so failing keeps the result identical (the planner routes here only when the table does
// not set ignore-parse-errors, i.e. Flink is in throw mode too).
#[test]
#[should_panic(expected = "unknown CDC operation")]
fn cdc_unknown_op_fails() {
    let unknown = br#"{"before":null,"after":{"id":9,"name":"z","score":9.5},"op":"x"}"#;
    MessageDecoder::new(6, json_schema(), "", "", 0, false, "").decode(&bodies(vec![Some(unknown.as_slice())]));
}

// A null "before" on an update fails (Flink's REPLICA_IDENTITY error), not a silent drop.
#[test]
#[should_panic(expected = "null \"before\"")]
fn cdc_debezium_null_before_update_fails() {
    let update = br#"{"before":null,"after":{"id":2,"name":"b","score":2.5},"op":"u"}"#;
    MessageDecoder::new(6, json_schema(), "", "", 0, false, "").decode(&bodies(vec![Some(update.as_slice())]));
}

// Skip mode (`ignore-parse-errors`): every per-message failure — malformed JSON, an unknown op, a
// null pre-image on an update — drops that message, and the surrounding good messages still decode,
// matching Flink's catch-everything-per-message skip.
#[test]
fn cdc_debezium_skip_mode_drops_undecodable_messages() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c"}"#;
    let malformed = br#"{"before":null,"after":{"id":2,"#;
    let unknown_op = br#"{"before":null,"after":{"id":3,"name":"x","score":3.5},"op":"x"}"#;
    let null_before = br#"{"before":null,"after":{"id":4,"name":"y","score":4.5},"op":"u"}"#;
    let delete = br#"{"before":{"id":5,"name":"c","score":5.5},"after":null,"op":"d"}"#;
    let body = bodies(vec![
        Some(insert.as_slice()),
        Some(malformed),
        Some(unknown_op),
        Some(null_before),
        Some(delete),
    ]);

    let out = MessageDecoder::new(6, json_schema(), "", "", 0, true, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 5]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 3]); // INSERT + DELETE; the three bad messages vanish
}

// Skip mode on the plain JSON decode (`json` + ignore-parse-errors): a malformed body or an
// unconvertible value drops only that message.
#[test]
fn json_skip_mode_drops_undecodable_messages() {
    let good = br#"{"id":1,"name":"a","score":1.5}"#;
    let malformed = br#"{"id":2,"name":"#;
    let bad_type = br#"{"id":"abc","name":"c","score":3.5}"#;
    let also_good = br#"{"id":4,"name":"d","score":4.5}"#;
    let body =
        bodies(vec![Some(good.as_slice()), Some(malformed), Some(bad_type), Some(also_good)]);

    let out = MessageDecoder::new(0, json_schema(), "", "", 0, true, "").decode(&body);

    // Flink's JSON ignore-parse-errors granularity (parity-pinned): a structurally bad document
    // drops the whole message, but a bad VALUE nulls just that field and keeps the row.
    assert_eq!(out.num_rows(), 3);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((id.value(0), id.value(2)), (1, 4));
    assert!(id.is_null(1));
    let name = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(1), "c");
}

// Skip mode with nothing to skip takes the batched fast path and decodes everything.
#[test]
fn json_skip_mode_clean_batch_decodes_in_full() {
    let a = br#"{"id":1,"name":"a","score":1.5}"#;
    let b = br#"{"id":2,"name":"b","score":2.5}"#;
    let body = bodies(vec![Some(a.as_slice()), Some(b)]);

    let out = MessageDecoder::new(0, json_schema(), "", "", 0, true, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
}

// OGG JSON (format 7): same nested before/after layout as Debezium, but the op field is `op_type`
// with I/U/D codes.
#[test]
fn cdc_ogg_dialect_uses_op_type() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op_type":"I"}"#;
    let update =
        br#"{"before":{"id":2,"name":"b","score":2.5},"after":{"id":2,"name":"b2","score":3.5},"op_type":"U"}"#;
    let delete = br#"{"before":{"id":3,"name":"c","score":4.5},"after":null,"op_type":"D"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

    let out = MessageDecoder::new(7, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4); // insert + (update→2) + delete
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 1, 2, 3]);
}

// Maxwell JSON (format 8): `{data, old, type}` — `data` is the full post-image, `old` only the
// changed fields. An update's UPDATE_BEFORE is coalesce(old, data) per field (unchanged fields fall
// back to `data`); a delete reads the row from `data`, not `old`.
#[test]
fn cdc_maxwell_merges_partial_old_image() {
    let insert = br#"{"data":{"id":1,"name":"a","score":1.5},"type":"insert"}"#;
    // Only `name` changed (b → b2): `old` carries just `name`; id/score must come from `data`.
    let update = br#"{"data":{"id":2,"name":"b2","score":2.5},"old":{"name":"b"},"type":"update"}"#;
    let delete = br#"{"data":{"id":3,"name":"c","score":3.5},"type":"delete"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

    let out = MessageDecoder::new(8, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    // UPDATE_BEFORE keeps the old name "b"; the unchanged id/score are pulled from `data`.
    assert_eq!((0..4).map(|i| names.value(i)).collect::<Vec<_>>(), vec!["a", "b", "b2", "c"]);
    let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5, 2.5, 3.5]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 1, 2, 3]);
}

// The DataOld pre-image follows KEY PRESENCE, not decoded nullability (Flink's findValue rule): a
// field present in `old` as an explicit null was changed FROM null and UPDATE_BEFORE keeps the
// null; an absent field is unchanged and copies from `data`.
#[test]
fn cdc_maxwell_keeps_explicit_null_in_old() {
    let update =
        br#"{"data":{"id":1,"name":"was-null","score":1.5},"old":{"name":null},"type":"update"}"#;
    let body = bodies(vec![Some(update.as_slice())]);

    let out = MessageDecoder::new(8, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert!(names.is_null(0)); // UPDATE_BEFORE: name was explicitly null before the update
    assert_eq!(names.value(1), "was-null");
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 1]); // absent from `old` → unchanged → copied from `data`
}

// Flink reads `old` unchecked on an update (row.getRow / old.getRow(i)), so a missing `old` or a
// Canal `old` array shorter than `data` is a corrupt message that fails the job — never a silent
// fall-back to the post-image.
#[test]
#[should_panic(expected = "null \"old\"/pre image")]
fn cdc_maxwell_update_without_old_fails() {
    let update = br#"{"data":{"id":1,"name":"x","score":1.5},"type":"update"}"#;
    let body = bodies(vec![Some(update.as_slice())]);
    MessageDecoder::new(8, json_schema(), "", "", 0, false, "").decode(&body);
}

#[test]
#[should_panic(expected = "\"old\" array is shorter")]
fn cdc_canal_uneven_update_arrays_fail() {
    let update = br#"{"data":[{"id":1,"name":"x","score":1.5},{"id":2,"name":"y","score":2.5}],"old":[{"name":"w"}],"type":"UPDATE"}"#;
    let body = bodies(vec![Some(update.as_slice())]);
    MessageDecoder::new(9, json_schema(), "", "", 0, false, "").decode(&body);
}

// Canal's findValue presence scan covers the WHOLE `old` array: a key present in any element
// counts as present for every paired element, exactly as Flink's oldField.findValue over the
// array node behaves.
#[test]
fn cdc_canal_presence_is_per_message_across_elements() {
    let update = br#"{"data":[{"id":1,"name":"a2","score":1.5},{"id":2,"name":"b2","score":2.5}],"old":[{"name":"a"},{"id":2}],"type":"UPDATE"}"#;
    let body = bodies(vec![Some(update.as_slice())]);

    let out = MessageDecoder::new(9, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4); // two elements, UB+UA each
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    // Element 0's UPDATE_BEFORE: `name` present in old[0] → "a"; `id` present in old[1] (message-
    // wide presence!) but null in old[0] → keeps old[0]'s null, not data's 1 — Flink's exact quirk.
    assert!(id.is_null(0));
    assert_eq!(names.value(0), "a");
    // Element 1's UPDATE_BEFORE: `id` from old[1] = 2; `name` null in old[1] but present message-
    // wide → keeps the null.
    assert_eq!(id.value(2), 2);
    assert!(names.is_null(2));
}

// Canal JSON (format 9): `data`/`old` are arrays, so one message fans out per element. An INSERT
// with a two-row `data` emits two INSERTs; an UPDATE pairs `data[i]` with `old[i]` and merges the
// partial `old` like Maxwell (UPDATE_BEFORE coalesces old over data).
#[test]
fn cdc_canal_fans_out_arrays_and_merges_old() {
    // One INSERT message carrying two rows.
    let insert = br#"{"data":[{"id":1,"name":"a","score":1.5},{"id":2,"name":"b","score":2.5}],"type":"INSERT"}"#;
    // One UPDATE message, one element: only `score` changed (3.5 → 3.75); id/name come from data.
    let update =
        br#"{"data":[{"id":3,"name":"c","score":3.75}],"old":[{"score":3.5}],"type":"UPDATE"}"#;
    // A CREATE (DDL) message is skipped entirely.
    let ddl = br#"{"data":null,"type":"CREATE"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(ddl)]);

    let out = MessageDecoder::new(9, json_schema(), "", "", 0, false, "").decode(&body);

    // 2 inserts + (update → UB + UA); CREATE dropped.
    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 3, 3]);
    let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!((0..4).map(|i| names.value(i)).collect::<Vec<_>>(), vec!["a", "b", "c", "c"]);
    let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
    // UPDATE_BEFORE keeps the old score 3.5; UPDATE_AFTER has the new 3.75.
    assert_eq!(scores.values(), &[1.5, 2.5, 3.5, 3.75]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 0, 1, 2]);
}

// Each input row is one complete JSON document; the decoder emits one typed row per document,
// matching the target schema's columns and order.
#[test]
fn json_decode_emits_one_row_per_document() {
    let batch = bodies(vec![
        Some(br#"{"id": 1, "name": "a", "score": 1.5}"#),
        Some(br#"{"id": 2, "name": "b", "score": 2.5}"#),
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 2);
    assert_eq!(values(&out, 0), vec![1, 2]);
    let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
}

// Fields absent from a document and a null body both yield SQL NULLs, not failures.
#[test]
fn json_decode_tolerates_missing_fields_and_null_bodies() {
    let batch = bodies(vec![
        Some(br#"{"id": 1}"#),
        None,
        Some(br#"{"id": 3, "name": "c", "score": 9.0}"#),
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    // A null body contributes no row; the present documents decode in order.
    assert_eq!(out.num_rows(), 2);
    assert_eq!(values(&out, 0), vec![1, 3]);
    assert!(out.column(1).is_null(0));
}

// An empty input batch flushes to an empty batch of the target schema, not a panic.
#[test]
fn json_decode_empty_batch_yields_empty() {
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&bodies(vec![]));
    assert_eq!(out.num_rows(), 0);
    assert_eq!(out.schema(), json_schema());
}

// Every scalar type the boundary admits decodes: numbers for the numeric widths (a float for an
// integer column truncates), true/false for BOOLEAN, and strings for DATE and for TIMESTAMP in
// both the SQL and ISO-8601 forms (a bare number is a raw nanosecond epoch).
#[test]
fn json_decode_covers_boundary_scalar_types() {
    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
        Int8Array, TimestampNanosecondArray,
    };
    use arrow::datatypes::TimeUnit;
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("day", DataType::Date32, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]));
    let batch = bodies(vec![
        Some(
            br#"{"i8": -3, "i16": 300, "i32": 70000, "i64": 5000000000, "f32": 1.5,
                    "f64": 2.5, "flag": true, "day": "2026-07-01", "ts": "2026-07-01 12:00:00.123"}"#,
        ),
        Some(
            br#"{"i8": 1.9, "i16": -2, "i32": 3, "i64": "42", "f32": 2, "f64": "Infinity",
                    "flag": "TRUE", "day": "1970-01-02", "ts": "2026-07-01 12:00:00.123Z"}"#,
        ),
        Some(br#"{"flag": 1}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 3);
    let i8s = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!((i8s.value(0), i8s.value(1)), (-3, 1)); // 1.9 truncates toward zero
    let i16s = out.column(1).as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!((i16s.value(0), i16s.value(1)), (300, -2));
    let i32s = out.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!((i32s.value(0), i32s.value(1)), (70000, 3));
    assert_eq!(values(&out, 3), vec![5000000000, 42, 0]);
    assert!(out.column(3).is_null(2));
    let f32s = out.column(4).as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!((f32s.value(0), f32s.value(1)), (1.5, 2.0));
    let f64s = out.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(f64s.value(0), 2.5);
    assert_eq!(f64s.value(1), f64::INFINITY); // Java's Double.parseDouble spelling
    let flags = out.column(6).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(flags.value(0) && flags.value(1)); // parseBoolean is case-insensitive
    assert!(!flags.value(2)); // ... and a number is simply false, never an error
    let days = out.column(7).as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(days.value(1), 1);
    let ts = out.column(8).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
    let expected = 1_782_907_200_123_000_000i64; // 2026-07-01T12:00:00.123Z
    // The trailing 'Z' is the tolerated LTZ shape (divergences/21) — same instant.
    assert_eq!((ts.value(0), ts.value(1)), (expected, expected));
    assert!(ts.is_null(2));
}

/// The SQL/ISO-8601 timestamp modes reject each other's separator, numbers fail a timestamp/date
/// column, and a float literal under a STRING column fails loudly (raw literal unrecoverable) —
/// the Flink envelope, per the JSON decode parity test.
#[test]
fn json_decode_rejects_off_mode_and_numeric_temporals() {
    use arrow::datatypes::TimeUnit;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let ts_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let sql = crate::json::JsonEnv::default();
    let iso = crate::json::JsonEnv { mode: flink_text::TimestampMode::Iso8601, lenient: false };
    let decode = |schema: &SchemaRef, env, body: &'static [u8]| {
        let schema = schema.clone();
        let batch = bodies(vec![Some(body)]);
        catch_unwind(AssertUnwindSafe(move || JsonDecoder::new(schema, env).decode(&batch)))
    };
    // SQL mode: space separator only; ISO mode: 'T' only (seconds optional there).
    assert!(decode(&ts_schema, sql, br#"{"ts": "2026-07-01T12:00:00"}"#)
        .is_err());
    assert!(decode(&ts_schema, iso, br#"{"ts": "2026-07-01 12:00:00"}"#)
        .is_err());
    assert!(decode(&ts_schema, iso, br#"{"ts": "2026-07-01T12:00"}"#)
        .is_ok());
    // A bare number is not a Flink timestamp or date.
    assert!(decode(&ts_schema, sql, br#"{"ts": 123456789}"#).is_err());
    let day_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("day", DataType::Date32, true)]));
    assert!(decode(&day_schema, sql, br#"{"day": 42}"#).is_err());
    assert!(decode(&day_schema, sql, br#"{"day": "2026-7-1"}"#).is_err());
    // STRING coercions: ints/bools/containers echo exactly, a float literal fails loudly.
    let str_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let echoed = decode(
        &str_schema,
        sql,
        b"{\"s\": {\"a\": 1, \"b\": [true, null, \"x\\n\"], \"a\": 2}}",
    )
    .unwrap();
    let strings = echoed.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    // Duplicate keys collapse last-value-first-position, like Jackson's tree; the escaped
    // newline round-trips through Jackson-style escaping.
    assert_eq!(strings.value(0), "{\"a\":2,\"b\":[true,null,\"x\\n\"]}");
    let echoed = decode(&str_schema, sql, br#"{"s": 42}"#).unwrap();
    let strings = echoed.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "42");
    assert!(decode(&str_schema, sql, br#"{"s": 1.5}"#).is_err());
}

// Nested ROW/ARRAY/MAP decode recursively: a null or missing struct nulls its children, list
// elements keep order and admit nulls, and map keys parse as the key column's type.
#[test]
fn json_decode_covers_nested_types() {
    use arrow::array::{ListArray, MapArray, StructArray};
    let nested = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("row", DataType::Struct(nested.clone()), true),
        Field::new(
            "nums",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new(
            "tags",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]));
    let batch = bodies(vec![
        Some(br#"{"row": {"a": 1, "b": "x"}, "nums": [1, null, 3], "tags": {"7": "seven"}}"#),
        Some(br#"{"row": {"a": 2}, "nums": [], "tags": {}}"#),
        Some(br#"{"row": null}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 3);

    let row = out.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let a = row.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((a.value(0), a.value(1)), (1, 2));
    assert!(row.column(1).is_null(1)); // missing nested field -> null
    assert!(row.is_null(2)); // null struct -> null row

    let nums = out.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    let first = nums.value(0);
    let first = first.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((first.value(0), first.value(2)), (1, 3));
    assert!(first.is_null(1));
    assert_eq!(nums.value_length(1), 0);
    assert!(nums.is_null(2)); // missing list -> null

    let tags = out.column(2).as_any().downcast_ref::<MapArray>().unwrap();
    let keys = tags.keys().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(keys.value(0), 7); // object key parsed as the BIGINT key type
    let map_values = tags.values().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(map_values.value(0), "seven");
    assert_eq!(tags.value_length(1), 0);
    assert!(tags.is_null(2));
}

// The decimal-bearing (raw-literals) path parses DECIMAL columns with Flink's exact semantics:
// the raw digit string (no f64 rounding), HALF_UP past the declared scale, and NULL — not an
// error — on precision overflow. arrow-json's own decimal parse truncates and errors, which
// silently diverged from Flink; the column decodes as raw text and converts here instead.
#[test]
fn json_decimal_rounds_half_up_and_nulls_on_overflow() {
    use arrow::array::Decimal128Array;
    let nested = Fields::from(vec![Field::new("p", DataType::Decimal128(5, 2), true)]);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Decimal128(5, 2), true),
        Field::new("wide", DataType::Decimal128(38, 20), true),
        Field::new("row", DataType::Struct(nested.clone()), true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"d": 1.235, "wide": 0.12345678901234567890123456789, "row": {"p": "-1.235"}}"#),
        Some(br#"{"d": 12345.6, "wide": null, "row": null}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let d = out.column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(d.value(0), 124); // HALF_UP, not truncation
    assert!(d.is_null(1)); // precision overflow → NULL (DecimalData.fromBigDecimal), not an error
    let wide = out.column(1).as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(wide.value(0), 12345678901234567890i128); // exact raw literal, HALF_UP at scale 20
    let row = out.column(2).as_any().downcast_ref::<StructArray>().unwrap();
    let p = row.column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(p.value(0), -124); // nested decimals get the same conversion; strings trim+parse
    assert!(row.is_null(1));
}

// Unknown keys are skipped and a duplicated field keeps its last value — Jackson (hence Flink)
// and arrow-json agree on both.
#[test]
fn json_decode_skips_unknown_keys_and_keeps_last_duplicate() {
    let batch =
        bodies(vec![Some(br#"{"extra": [1, {"x": 2}], "id": 1, "name": "a", "id": 5}"#)]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(values(&out, 0), vec![5]);
}

// DECIMAL columns route to the raw-literal (arrow-json) path: a number with more significant
// digits than an f64 carries still decodes exactly, in number and string position alike.
#[test]
fn json_decode_decimal_stays_exact_beyond_f64_precision() {
    let schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("d", DataType::Decimal128(30, 10), true)]));
    let batch = bodies(vec![
        Some(br#"{"d": 12345678901234567.8901234567}"#),
        Some(br#"{"d": "12345678901234567.8901234567"}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let d = out.column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();
    let exact = 123456789012345678901234567i128;
    assert_eq!((d.value(0), d.value(1)), (exact, exact));
}

#[test]
#[should_panic(expected = "as Int64")]
fn json_decode_rejects_type_mismatch() {
    let batch = bodies(vec![Some(br#"{"id": true}"#)]);
    JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
}

#[test]
#[should_panic(expected = "single object")]
fn json_decode_rejects_non_object_document() {
    let batch = bodies(vec![Some(br#"[{"id": 1}]"#)]);
    JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
}

// OVER (ORDER BY rt RANGE UNBOUNDED PRECEDING) running SUM: ties in rt share the post-fold value,
// and the running total persists across update calls.
#[test]
fn over_running_sum_shares_range_ties() {
    let rt: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 1000, 1000, 2000]));
    let value: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("rt", DataType::Int64, false),
        Field::new("value0", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![rt, value]).unwrap();
    let mut over = OverAggregator::new(vec![0], vec![0]); // bigint value, SUM
    // rt 1000 ties (20,30) both see 10+20+30=60; emitted in input order.
    assert_eq!(values(&over.update(&batch), 0), vec![10, 60, 60, 100]);

    // A later complete batch continues the running total (UNBOUNDED PRECEDING).
    let rt2: ArrayRef = Arc::new(Int64Array::from(vec![3000i64]));
    let value2: ArrayRef = Arc::new(Int64Array::from(vec![5i64]));
    let batch2 = RecordBatch::try_new(schema, vec![rt2, value2]).unwrap();
    assert_eq!(values(&over.update(&batch2), 0), vec![105]);
}

// PARTITION BY: each key has its own running SUM; rt ties within a key share the value.
#[test]
fn over_running_sum_per_partition_key() {
    let rt: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 0, 1000, 1000, 2000]));
    let value: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 100, 20, 30, 40]));
    let key0: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1, 1, 2]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rt", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, false),
        ])),
        vec![rt, value, key0],
    )
    .unwrap();
    let mut over = OverAggregator::new(vec![0], vec![0]);
    // key 1: 10, then (20,30) tie -> 60, 60; key 2: 100, then 140.
    assert_eq!(values(&over.update(&batch), 0), vec![10, 100, 60, 60, 140]);
}

// The columnar (buffering) OVER passes input columns through and appends the running aggregate,
// emitting only the rows the watermark has completed.
#[test]
fn over_window_buffers_and_passes_through() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 2, 1]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 100, 40]));
    // rowtime in nanoseconds (millis 0, 1000, 500, 9000).
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        500_000_000,
        9_000_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false);
    over.push(batch);
    // Watermark 2000ms completes the first three rows (rt 0/1000/500); the rt=9000 row stays.
    let out = over.flush(2000).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(values(&out, 0), vec![1, 1, 2]); // k passed through
    assert_eq!(values(&out, 1), vec![10, 20, 100]); // v passed through
    // running SUM per key: key 1 -> 10, 30; key 2 -> 100 (result is the last column).
    assert_eq!(values(&out, 3), vec![10, 30, 100]);
    // The pending row flushes once the watermark passes it.
    let rest = over.flush(10_000).unwrap();
    assert_eq!(rest.num_rows(), 1);
    assert_eq!(values(&rest, 1), vec![40]); // v
    assert_eq!(values(&rest, 3), vec![70]); // key 1 running sum 10+20+40
}

#[test]
fn over_state_partitions_and_restores_by_flink_key_group() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(Int64Array::from(vec![10i64, 20])),
            Arc::new(TimestampNanosecondArray::from(vec![0i64, 0])),
        ],
    )
    .unwrap();
    let mut before = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false);
    before.push(batch).unwrap();
    let groups = before.snapshot_key_groups(128, &[-1]);
    assert!(groups.len() >= 2, "test keys should cover distinct raw key groups");
    let snapshots: Vec<Vec<u8>> = groups
        .iter()
        .map(|&group| before.snapshot_key_group(group, 128, &[-1]))
        .collect();
    let mut restored = OverWindowAggregator::restore_partitions(
        vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false, &snapshots,
    );
    let out = restored.flush(0).unwrap();
    let mut rows: Vec<(i64, i64)> = values(&out, 0)
        .into_iter()
        .zip(values(&out, 3))
        .collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
}

// Bounded ROWS frame (1 PRECEDING): each row's SUM covers only itself and the row before it
// within its partition, recomputed over the frame slice — and the trailing edge drops older rows.
#[test]
fn bounded_rows_over_sums_the_frame_slice() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1, 2]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 100]));
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        2_000_000_000,
        500_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
    // frame_kind 1 = bounded ROWS, offset 1 = one preceding row.
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 1, 1, false);
    over.push(batch);
    let out = over.flush(2000).unwrap();
    assert_eq!(out.num_rows(), 4);
    // SUM over {self, prev}: key 1 -> 10, 10+20, 20+30; key 2 (lone row) -> 100.
    assert_eq!(values(&out, 1), vec![10, 20, 30, 100]); // v passed through
    assert_eq!(values(&out, 3), vec![10, 30, 50, 100]);
}

// Bounded RANGE frame (1 SECOND PRECEDING): each row's SUM covers the rows within 1000ms of it,
// by rowtime interval rather than a physical row count.
#[test]
fn bounded_range_over_sums_the_time_interval() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        2_000_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
    // frame_kind 2 = bounded RANGE, offset 1000 = a 1000ms preceding interval.
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 2, 1000, false);
    over.push(batch);
    let out = over.flush(2000).unwrap();
    assert_eq!(out.num_rows(), 3);
    // SUM over rows within 1000ms: rt0 -> {10}, rt1000 -> {10,20}, rt2000 -> {20,30}.
    assert_eq!(values(&out, 3), vec![10, 30, 50]);
}

// Proctime OVER: rows fold in arrival order and emit immediately (no watermark). The running SUM
// per key advances row by row in the order the rows arrive.
#[test]
fn proctime_over_running_sum_in_arrival_order() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 100, 20]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v]).unwrap();
    // rt_column is ignored in proctime mode (arrival order); value col 1, key col 0, unbounded.
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 0, vec![1], vec![0], 0, 0, true);
    let out = over.push_proctime(batch).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(values(&out, 1), vec![10, 100, 20]); // v passed through
    assert_eq!(values(&out, 2), vec![10, 100, 30]); // running SUM per key, in arrival order
}

// Independent value columns in one OVER group: SUM(v0) and MAX(v1) read different input columns.
#[test]
fn over_independent_value_columns() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1]));
    let v0: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
    let v1: ArrayRef = Arc::new(Int64Array::from(vec![5i64, 15, 10]));
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        2_000_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v0", DataType::Int64, true),
        Field::new("v1", DataType::Int64, true),
        Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v0, v1, rt]).unwrap();
    // value types [bigint, bigint]; columns [1, 2]; kinds [SUM, MAX]; rt col 3; key col 0; unbounded.
    let mut over =
        OverWindowAggregator::new(vec![0, 0], vec![0, 2], 3, vec![1, 2], vec![0], 0, 0, false);
    over.push(batch);
    let out = over.flush(2000).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(values(&out, 4), vec![10, 30, 60]); // running SUM(v0)
    assert_eq!(values(&out, 5), vec![5, 15, 15]); // running MAX(v1)
}

// Two-phase cumulative: per-slice SUM partials merge into the nested windows of their bucket.
#[test]
fn cumulative_two_phase_merges_nested_windows() {
    // max size 3 s, step 1 s, cumulative, bigint value, SUM.
    let mut agg = TumblingAggregator::new(3000, 1000, true, vec![0], vec![0]);
    let partial = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("partial0", DataType::Int64, true),
            Field::new("slice_end", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 1, 1])),
            Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            Arc::new(Int64Array::from(vec![1000i64, 2000, 3000])),
        ],
    )
    .unwrap();
    agg.update_partial(&partial).unwrap();
    let out = agg.flush(3000);
    // Nested windows share the bucket start 0; each accumulates the slices up to its end:
    // (0,1000]=10, (0,2000]=10+20=30, (0,3000]=10+20+30=60.
    assert_eq!(values(&out, 1), vec![0, 0, 0]); // window_start
    assert_eq!(values(&out, 2), vec![1000, 2000, 3000]); // window_end
    assert_eq!(values(&out, 3), vec![10, 30, 60]); // running SUM
}

// Window-attached local half (q5): rows carry explicit window_start/window_end (epoch millis)
// instead of a rowtime to slice; each folds into the one window it names, and flush_partial emits
// the per-window partial keyed by window end. No late-data drop — a row whose window the watermark
// has already reached still folds (the upstream emits it exactly at that watermark).
#[test]
fn window_attached_local_folds_per_named_window() {
    // SUM over bigint, no grouping key (grouped only by window). window/slide are unused by the
    // attached ingest, so their values are immaterial.
    let mut agg = TumblingAggregator::new(10000, 10000, false, vec![0], vec![0]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0i64, 0, 2000])),
            Arc::new(Int64Array::from(vec![10000i64, 10000, 12000])),
            Arc::new(Int64Array::from(vec![3i64, 5, 7])),
        ],
    )
    .unwrap();
    agg.update_attached(&batch).unwrap();
    let out = agg.flush_partial(20000);
    // Output columns: [partial0, slice_end]. Windows emitted in ascending end order.
    assert_eq!(values(&out, 1), vec![10000, 12000]); // slice_end == the named window ends
    assert_eq!(values(&out, 0), vec![8, 7]); // (0,10000] sums 3+5, (2000,12000] sums 7
}

// A `[ts, value0, key0]` batch (bigint value and key) for the memory-accounting tests.
fn keyed_window_batch(ts_millis: i64, keys: Vec<i64>) -> RecordBatch {
    let n = keys.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![ts_millis; n])),
            Arc::new(Int64Array::from(vec![1i64; n])),
            Arc::new(Int64Array::from(keys)),
        ],
    )
    .unwrap()
}

// Open-window state grows the pool reservation, tracks the full-scan footprint exactly, and
// returns to zero when the windows close — the release-on-close half of memory accounting.
#[test]
fn window_state_reserves_and_releases_memory() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut agg = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0])
        .with_memory_pool(&pool)
        .unwrap();
    agg.update(&keyed_window_batch(0, (0..50).collect())).unwrap();
    agg.update(&keyed_window_batch(1500, (0..20).collect())).unwrap();
    assert!(pool.reserved() > 0);
    assert_eq!(agg.memory.state_bytes, agg.computed_state_bytes()); // incremental tracking must not drift
    let both_windows = pool.reserved();

    agg.flush(1000); // closes the first window only
    assert_eq!(agg.memory.state_bytes, agg.computed_state_bytes());
    assert!(pool.reserved() > 0 && pool.reserved() < both_windows);

    agg.flush(2000); // closes the rest
    assert_eq!(pool.reserved(), 0);
    drop(agg);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn window_state_partitions_and_restores_by_flink_key_group() {
    let mut before = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0]);
    before.update(&keyed_window_batch(0, vec![1, 2])).unwrap();
    let groups = before.snapshot_key_groups(128, &[-1]);
    assert!(!groups.is_empty());
    let snapshots: Vec<Vec<u8>> = groups
        .iter()
        .map(|&group| before.snapshot_key_group(group, 128, &[-1]))
        .collect();

    let mut restored =
        TumblingAggregator::restore_partitions(1000, 1000, false, vec![0], vec![0], &snapshots);
    let out = restored.flush(1000);
    let keys = out
        .column_by_name("key0")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    let sums = out
        .column_by_name("result0")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(keys, vec![1, 2]);
    assert_eq!(sums, vec![1, 1]);
}

// Exceeding the budget is a clear, attributable failure — not a container OOM.
#[test]
fn window_state_over_budget_fails_clearly() {
    let mut agg = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0])
        .with_memory_budget(256)
        .unwrap();
    let err = agg.update(&keyed_window_batch(0, (0..100).collect())).unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

// Every rolled-out state shape enforces its budget: exceeding it is an error, not an overrun.
// One test per shape (accumulator maps, byte-row maps, buffered batches, bounded buffers).
#[test]
fn session_state_over_budget_fails_clearly() {
    let mut agg = SessionAggregator::new(1000, vec![0], vec![0])
        .with_memory_budget(256)
        .unwrap();
    let err = agg.update(&keyed_window_batch(0, (0..100).collect())).unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

#[test]
fn session_state_partitions_and_restores_by_flink_key_group() {
    let mut before = SessionAggregator::new(1000, vec![0], vec![0]);
    before.update(&keyed_window_batch(0, vec![1, 2])).unwrap();
    let groups = before.snapshot_key_groups(128, &[-1]);
    assert!(groups.len() >= 2, "test keys should cover distinct raw key groups");
    let snapshots: Vec<Vec<u8>> = groups
        .iter()
        .map(|&group| before.snapshot_key_group(group, 128, &[-1]))
        .collect();

    let mut restored = SessionAggregator::restore_partitions(1000, vec![0], vec![0], &snapshots);
    let out = restored.flush(1000);
    assert_eq!(values(&out, 0), vec![1, 2]);
    assert_eq!(values(&out, 3), vec![1, 1]);
}

#[test]
fn group_state_over_budget_fails_and_deletes_release() {
    // A generous budget: inserts fit, and retracting every record shrinks the tracking to zero.
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true)
        .with_memory_budget(1 << 20)
        .unwrap();
    agg.update(&group_changelog(vec![1, 2], vec![Some(10), Some(20)], vec![0, 0])).unwrap();
    assert!(agg.memory.state_bytes > 0);
    agg.update(&group_changelog(vec![1, 2], vec![Some(10), Some(20)], vec![3, 3])).unwrap();
    assert_eq!(agg.memory.state_bytes, 0); // both groups deleted -> fully released

    let mut tight = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true)
        .with_memory_budget(128)
        .unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<Option<i64>> = keys.iter().map(|&k| Some(k)).collect();
    let err = tight
        .update(&group_changelog(keys, values, vec![0; 100]))
        .unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

#[test]
fn dedup_state_over_budget_fails_clearly() {
    // Keep-last over distinct keys stores one row per key; 100 keys cannot fit 64 bytes.
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, true, false, false)
        .with_memory_budget(64)
        .unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<i64> = (0..100).collect();
    let rts: Vec<i64> = vec![0; 100];
    let err = dedup.push(&join_batch(keys, values, rts)).unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

#[test]
fn sort_buffer_over_budget_fails_and_flush_releases() {
    let mut sorter = TemporalSorter::new(2).with_memory_budget(1 << 20).unwrap();
    sorter.push(join_batch(vec![1, 2], vec![10, 20], vec![0, 1000])).unwrap();
    assert!(sorter.memory.state_bytes > 0);
    sorter.flush(i64::MAX);
    assert_eq!(sorter.memory.state_bytes, 0); // everything emitted -> buffer released

    let mut tight = TemporalSorter::new(2).with_memory_budget(16).unwrap();
    let err = tight.push(join_batch(vec![1], vec![10], vec![0])).unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

#[test]
fn interval_join_buffers_over_budget_fail_clearly() {
    let mut joiner = inner_interval_joiner(-1000, 1000).with_memory_budget(16).unwrap();
    let err = joiner.push_left(join_batch(vec![1], vec![10], vec![0]), None).unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

// The hash join the operator delegates to DataFusion runs under the operator's pool, so its
// transient build side draws on the same budget as the buffered state: a buffer that fits can
// still fail at join time when the build side does not.
#[test]
fn join_working_memory_draws_on_the_operator_budget() {
    let n = 20_000usize;
    let keys: Vec<i64> = vec![1; n];
    let values: Vec<i64> = (0..n as i64).collect();
    let rts: Vec<i64> = vec![0; n];
    let big = join_batch(keys, values, rts);
    let budget = (big.get_array_memory_size() + (64 << 10)) as i64;

    let mut joiner = inner_interval_joiner(-1000, 1000).with_memory_budget(budget).unwrap();
    joiner.push_left(big, None).unwrap(); // buffers fit the budget
    let err =
        joiner.push_right(join_batch(vec![1], vec![100], vec![0]), None).unwrap_err();
    assert!(err.to_string().contains("join working memory"), "{err}");
}

#[test]
fn local_group_state_over_budget_fails_and_flush_releases() {
    let mut agg = LocalGroupAggregator::new(vec![0], vec![0], vec![1], vec![], vec![0], vec![])
        .with_memory_budget(1 << 20)
        .unwrap();
    agg.update(&join_batch(vec![1, 2], vec![10, 20], vec![0, 0])).unwrap();
    assert!(agg.memory.state_bytes > 0);
    agg.flush();
    assert_eq!(agg.memory.state_bytes, 0); // the mini-batch drained -> fully released

    let mut tight = LocalGroupAggregator::new(vec![0], vec![0], vec![1], vec![], vec![0], vec![])
        .with_memory_budget(64)
        .unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<i64> = (0..100).collect();
    let err = tight.update(&join_batch(keys, values, vec![0; 100])).unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

#[test]
fn updating_join_state_over_budget_fails_and_retract_releases() {
    let mut joiner = inner_joiner().with_memory_budget(1 << 20).unwrap();
    joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap();
    assert!(joiner.memory.state_bytes > 0);
    joiner.push(&changelog_join_batch(vec![1], vec![10], vec![3]), true).unwrap();
    assert_eq!(joiner.memory.state_bytes, 0); // the only stored row retracted -> released

    let mut tight = inner_joiner().with_memory_budget(64).unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<i64> = (0..100).collect();
    let err = tight
        .push(&changelog_join_batch(keys, values, vec![0; 100]), true)
        .unwrap_err();
    assert!(err.to_string().contains("managed-memory budget"), "{err}");
}

#[test]
fn topn_buffer_stays_within_budget_under_eviction() {
    // A bounded Top-3 keeps its reservation bounded no matter how many rows stream through.
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 3, false, false)
        .with_memory_budget(1 << 20)
        .unwrap();
    for i in 0..50 {
        ranker.push(&topn_batch(vec![1], vec![i])).unwrap();
    }
    let bounded = ranker.memory.state_bytes;
    for i in 50..100 {
        ranker.push(&topn_batch(vec![1], vec![i])).unwrap();
    }
    assert_eq!(ranker.memory.state_bytes, bounded); // eviction keeps the tracked state flat
}

#[test]
fn topn_net_diff_staging_is_accounted_and_released_on_flush() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 3, false, true)
        .with_memory_budget(1 << 20)
        .unwrap();
    ranker.push(&topn_batch(vec![1, 1, 2], vec![5, 3, 7])).unwrap();
    assert_eq!(ranker.staged_partitions(), 2);
    assert!(ranker.staging_bytes() > 0);
    let bundled = ranker.memory.state_bytes;

    ranker.flush_net_diff();
    assert_eq!(ranker.staged_partitions(), 0);
    assert_eq!(ranker.staging_bytes(), 0);
    assert!(ranker.memory.state_bytes < bundled);
}

// A restored snapshot is accounted the moment the budget attaches, so state that no longer fits
// fails at restore rather than silently exceeding the budget.
#[test]
fn restored_state_is_accounted_against_budget() {
    let mut agg = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0]);
    agg.update(&keyed_window_batch(0, (0..100).collect())).unwrap();
    let snapshot = agg.snapshot();
    let restored = TumblingAggregator::restore(1000, 1000, false, vec![0], vec![0], &snapshot);
    assert!(restored.with_memory_budget(256).is_err());

    let restored = TumblingAggregator::restore(1000, 1000, false, vec![0], vec![0], &snapshot);
    let fits = restored.with_memory_budget(1 << 20).unwrap();
    assert_eq!(fits.memory.state_bytes, fits.computed_state_bytes());
    assert!(fits.memory.state_bytes > 0);
}

// A `[key0, value0, $row_kind$]` changelog batch (key/value bigint) for the GROUP BY tests;
// `kinds` is the RowKind byte per row (0 +I, 1 -U, 2 +U, 3 -D).
fn group_changelog(keys: Vec<i64>, values: Vec<Option<i64>>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

// All-INSERT convenience for the append-only tests.
fn group_batch(keys: Vec<i64>, values: Vec<i64>) -> RecordBatch {
    let kinds = vec![0i8; keys.len()];
    group_changelog(keys, values.into_iter().map(Some).collect(), kinds)
}

fn row_kinds(batch: &RecordBatch) -> Vec<i8> {
    batch
        .column_by_name(ROW_KIND_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}

// GROUP BY changelog: a key's first row emits INSERT(0); a later row that changes the result
// emits UPDATE_BEFORE(1)+UPDATE_AFTER(2); a row that leaves the result unchanged emits nothing.
#[test]
fn group_by_emits_insert_then_update_changelog() {
    // SUM(bigint) over value column 1, grouping on key column 0, emitting -U.
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    // keys a,a,b,a with values 1,2,5,0 — the last adds 0, leaving a's sum at 3 (suppressed).
    let out = agg.update(&group_batch(vec![1, 1, 2, 1], vec![1, 2, 5, 0])).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 0]);
    assert_eq!(values(&out, 0), vec![1, 1, 1, 2]); // key
    assert_eq!(values(&out, 1), vec![1, 1, 3, 5]); // running sum (prev on -U, new on +U)
}

// COUNT(*) (no argument column) counts every row, alongside a SUM over a value column.
#[test]
fn group_by_counts_every_row_for_count_star() {
    // kinds COUNT(*), SUM; COUNT(*) has no column (-1), SUM reads column 1; group on column 0.
    let mut agg = GroupAggregator::new(vec![3, 0], vec![0, 0], vec![-1, 1], vec![0], true);
    let out = agg.update(&group_batch(vec![1, 1], vec![10, 5])).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I, then -U/+U
    assert_eq!(values(&out, 1), vec![1, 1, 2]); // COUNT(*): 1, then 1->2
    assert_eq!(values(&out, 2), vec![10, 10, 15]); // SUM: 10, then 10->15
}

// AVG(bigint) keeps a running sum + non-null count and emits sum/count with integer division
// truncating toward zero (Flink's AvgAggFunction), retracting the prior average on each change.
#[test]
fn group_by_avg_truncates_toward_zero() {
    let mut agg = GroupAggregator::new(vec![4], vec![0], vec![1], vec![0], true);
    // One key, values 10 then 1 → avg 10, then 11/2 = 5 (truncated from 5.5, not rounded).
    let out = agg.update(&group_batch(vec![1, 1], vec![10, 1])).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I, then -U/+U
    assert_eq!(values(&out, 1), vec![10, 10, 5]);
}

// COUNT(*) FILTER (WHERE flag): a row folds into the aggregate only where its filter boolean is
// TRUE — FALSE and NULL are skipped, matching SQL FILTER.
#[test]
fn group_by_filter_gates_each_aggregate() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1]));
    let flag: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("flag", DataType::Boolean, true),
        ])),
        vec![key, flag],
    )
    .unwrap();
    // COUNT(*) over a boolean filter in column 1; group on column 0.
    let mut agg = GroupAggregator::new(vec![3], vec![0], vec![-1], vec![0], true)
        .with_filter_columns(vec![1]);
    let out = agg.update(&batch).unwrap();
    // Only the TRUE row counts → +I count=1; the FALSE/NULL rows leave it unchanged (suppressed).
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![1]);
}

// MIN/MAX over a string column: the Extremes multiset orders entries byte-lexicographically
// (Rust String Ord), retracting the prior extreme as it changes.
#[test]
fn group_by_min_max_string() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1]));
    let s: ArrayRef =
        Arc::new(StringArray::from(vec![Some("banana"), Some("apple"), Some("cherry")]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ])),
        vec![key, s],
    )
    .unwrap();
    // MIN, MAX over the string column 1; group on column 0; value type code 3 (Utf8).
    let mut agg = GroupAggregator::new(vec![1, 2], vec![3, 3], vec![1, 1], vec![0], true);
    let out = agg.update(&batch).unwrap();
    let last = out.num_rows() - 1;
    let min = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let max = out.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(min.value(last), "apple");
    assert_eq!(max.value(last), "cherry");
}

// A columnar input from an insert-only producer has no `$row_kind$` column; every row is then an
// INSERT (so the GROUP BY still emits its +I / -U / +U changelog).
#[test]
fn group_by_treats_absent_row_kind_as_insert() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 1])),
            Arc::new(Int64Array::from(vec![10i64, 20])),
        ],
    )
    .unwrap();
    let out = agg.update(&batch).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I(10); -U(10)/+U(30)
    assert_eq!(values(&out, 1), vec![10, 10, 30]);
}

// With the host's update-before flag off, an update emits only the UPDATE_AFTER row.
#[test]
fn group_by_omits_update_before_when_disabled() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], false);
    let out = agg.update(&group_batch(vec![1, 1], vec![10, 5])).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 2]); // +I(10), +U(15)
    assert_eq!(values(&out, 1), vec![10, 15]);
}

// A checkpoint preserves per-key state: a restored key is not "first", so a new row updates
// rather than re-inserting.
#[test]
fn group_by_survives_snapshot_restore() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    agg.update(&group_batch(vec![1], vec![10]));
    let snapshot = agg.snapshot();
    let mut restored =
        GroupAggregator::restore(vec![0], vec![0], vec![1], vec![0], true, &snapshot);
    let out = restored.update(&group_batch(vec![1], vec![5])).unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10), +U(15) — continues from 10
    assert_eq!(values(&out, 1), vec![10, 15]);
}

// Consuming a changelog: a -U input retracts a prior value, updating the running SUM.
#[test]
fn group_by_retracts_changelog_input() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    // +I 10, +I 20 (sum 30), then -U 10 (retract -> sum 20), all key 1.
    let out = agg.update(&group_changelog(
        vec![1, 1, 1],
        vec![Some(10), Some(20), Some(10)],
        vec![0, 0, 1],
    )).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2]);
    assert_eq!(values(&out, 1), vec![10, 10, 30, 30, 20]); // +I10; -U10/+U30; -U30/+U20
}

// Retracting a key's last record empties the group and emits a DELETE.
#[test]
fn group_by_deletes_when_last_record_retracted() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    let out = agg.update(&group_changelog(vec![1, 1], vec![Some(10), Some(10)], vec![0, 3])).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 3]); // +I(10), then -D(10)
    assert_eq!(values(&out, 1), vec![10, 10]);
}

// A SUM reports NULL once its last non-null value is retracted while a null-valued row keeps the
// group alive — matching the host's sum-with-retract.
#[test]
fn group_by_sum_is_null_after_last_value_retracted() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    // +I 5, +I NULL (sum still 5, suppressed), -U 5 (no non-null left -> SUM NULL, group alive).
    let out = agg.update(&group_changelog(
        vec![1, 1, 1],
        vec![Some(5), None, Some(5)],
        vec![0, 0, 1],
    )).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I(5); -U(5)/+U(NULL)
    let result = out.column(1);
    assert_eq!(result.len(), 3);
    assert!(!result.is_null(0) && result.as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 5);
    assert!(result.is_null(2)); // the +U carries a NULL sum
}

// MIN over a changelog: retracting the current minimum reveals the next-smallest from the
// per-key value multiset (what a single running value could not do).
#[test]
fn group_by_min_recovers_next_after_retract() {
    // kind MIN (1) over value column 1, group on column 0, emit -U.
    let mut agg = GroupAggregator::new(vec![1], vec![0], vec![1], vec![0], true);
    // +I 5, +I 3, +I 8 (min 3), then -U 3 (min back to 5).
    let out = agg.update(&group_changelog(
        vec![1, 1, 1, 1],
        vec![Some(5), Some(3), Some(8), Some(3)],
        vec![0, 0, 0, 1],
    )).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2]);
    // min: 5; 5->3; (8 leaves min 3, suppressed); 3->5 after retracting the 3.
    assert_eq!(values(&out, 1), vec![5, 5, 3, 3, 5]);
}

// The MIN/MAX value multiset survives a checkpoint, so a post-restore retract still recovers the
// next extreme.
#[test]
fn group_by_min_multiset_survives_snapshot_restore() {
    let mut agg = GroupAggregator::new(vec![1], vec![0], vec![1], vec![0], true);
    agg.update(&group_changelog(vec![1, 1], vec![Some(5), Some(3)], vec![0, 0])); // min 3
    let snapshot = agg.snapshot();
    let mut restored =
        GroupAggregator::restore(vec![1], vec![0], vec![1], vec![0], true, &snapshot);
    // Retract the 3 — the restored multiset still holds the 5, so the min becomes 5.
    let out = restored.update(&group_changelog(vec![1], vec![Some(3)], vec![1])).unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(3), +U(5)
    assert_eq!(values(&out, 1), vec![3, 5]);
}

// A `[p, s, $row_kind$]` insert-only batch (partition p at col 0, sort key s at col 1) for the
// Top-N tests.
fn topn_batch(p: Vec<i64>, s: Vec<i64>) -> RecordBatch {
    let kinds = vec![0i8; p.len()];
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, false),
            Field::new("s", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(p)),
            Arc::new(Int64Array::from(s)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

fn asc(index: usize) -> SortColumn {
    SortColumn { index, ascending: true, nulls_first: false }
}

// Top-2 by ascending sort key, one partition: a row entering the top-2 inserts and displaces the
// current 2nd (a DELETE); a row that would rank 3rd emits nothing.
#[test]
fn topn_keeps_smallest_n_per_partition() {
    // partition col 0, ORDER BY col 1 ASC, limit 2.
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, false);
    // s = 5, 3, 8, 1 for partition 1.
    let out = ranker.push(&topn_batch(vec![1, 1, 1, 1], vec![5, 3, 8, 1])).unwrap();
    // 5: +I5. 3: +I3 (top2 = {3,5}). 8: rank 3 -> nothing. 1: +I1, -D5 (top2 = {1,3}).
    assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
    assert_eq!(values(&out, 1), vec![5, 3, 5, 1]); // the sort-key column of each emitted row
}

// Top-2 with the rank number projected: a row entering shifts the rows below it, emitting the
// UPDATE_BEFORE/UPDATE_AFTER cascade Flink does, and an INSERT for a brand-new rank.
#[test]
fn topn_with_rank_number_emits_cascade() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, true, false);
    let out = ranker.push(&topn_batch(vec![1, 1, 1, 1], vec![5, 3, 8, 1])).unwrap();
    // 5: +I(5,1). 3: -U(5,1) +U(3,1) +I(5,2). 8: rank 3 -> nothing.
    // 1: -U(3,1) +U(1,1) -U(5,2) +U(3,2)  [5 pushed past rank 2, retracted by the -U].
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 0, 1, 2, 1, 2]);
    assert_eq!(values(&out, 1), vec![5, 5, 3, 5, 3, 1, 5, 3]); // sort-key column
    assert_eq!(values(&out, 2), vec![1, 1, 1, 2, 1, 1, 2, 2]); // appended rank (w0$o0)
}

// Partitions are independent: each keeps its own top-N.
#[test]
fn topn_is_per_partition() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 1, false, false);
    let out = ranker.push(&topn_batch(vec![1, 2, 1], vec![5, 7, 3])).unwrap();
    // p1: +I5; p2: +I7; p1 sees 3 < 5 -> -D5 then +I3 (delete first, as the host emits).
    assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
    assert_eq!(values(&out, 0), vec![1, 2, 1, 1]); // partition of each emitted row
    assert_eq!(values(&out, 1), vec![5, 7, 5, 3]);
}

// Net-diff (mini-batch) mode collapses the same batch to the per-partition net change: the same
// four rows that cascade eight changelog entries above emit only the final top-2 state diff.
#[test]
fn topn_net_diff_emits_batch_delta_with_rank() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, true, true);
    let pending = ranker.push(&topn_batch(vec![1, 1], vec![5, 3])).unwrap();
    assert_eq!(pending.num_rows(), 0);
    let pending = ranker.push(&topn_batch(vec![1, 1], vec![8, 1])).unwrap();
    assert_eq!(pending.num_rows(), 0);
    let out = ranker.flush_net_diff();
    // Fresh partition: old top empty, new top = {1@rank1, 3@rank2} — two inserts, no cascade.
    assert_eq!(row_kinds(&out), vec![0, 0]);
    assert_eq!(values(&out, 1), vec![1, 3]);
    assert_eq!(values(&out, 2), vec![1, 2]);

    // Second batch: 2 enters at rank 2 (1 stays at rank 1) — one -U/+U pair, rank 1 untouched.
    ranker.push(&topn_batch(vec![1], vec![2])).unwrap();
    let out = ranker.flush_net_diff();
    assert_eq!(row_kinds(&out), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![3, 2]);
    assert_eq!(values(&out, 2), vec![2, 2]);
}

// Net-diff without the rank number: the diff is top-N membership — leavers delete, entrants insert.
#[test]
fn topn_net_diff_emits_membership_delta() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, true);
    ranker.push(&topn_batch(vec![1, 1], vec![5, 3])).unwrap();
    ranker.push(&topn_batch(vec![1, 1], vec![8, 1])).unwrap();
    let out = ranker.flush_net_diff();
    // New partition: final top-2 = {1, 3}; the transient 5 never surfaces.
    assert_eq!(row_kinds(&out), vec![0, 0]);
    assert_eq!(values(&out, 1), vec![1, 3]);

    // 2 displaces 3: one -D and one +I; a batch that changes nothing emits nothing.
    ranker.push(&topn_batch(vec![1], vec![2])).unwrap();
    let out = ranker.flush_net_diff();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    assert_eq!(values(&out, 1), vec![3, 2]);
    ranker.push(&topn_batch(vec![1], vec![9])).unwrap();
    let out = ranker.flush_net_diff();
    assert_eq!(out.num_rows(), 0);
}

// The bounded buffer survives a checkpoint, so post-restore ranking continues correctly.
#[test]
fn topn_buffer_survives_snapshot_restore() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, false);
    ranker.push(&topn_batch(vec![1, 1], vec![5, 3])); // top2 = {3, 5}
    let snapshot = ranker.snapshot();
    let mut restored = TopNRanker::restore(vec![0], vec![asc(1)], 2, false, false, &snapshot);
    // A 1 enters the restored top-2 and displaces the 5.
    let out = restored.push(&topn_batch(vec![1], vec![1])).unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]); // -D5, +I1
    assert_eq!(values(&out, 1), vec![5, 1]);
}

// The `[k, v]` data schema (no `$row_kind$`) both sides carry in the updating-join tests.
fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn inner_joiner() -> UpdatingJoiner {
    UpdatingJoiner::new(vec![0], vec![0], JoinKind::Inner, kv_schema(), kv_schema(), None)
}

// A `[k, v, $row_kind$]` changelog batch (k join key at col 0) for the updating-join tests.
fn changelog_join_batch(k: Vec<i64>, v: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

// INNER updating join on column 0: a matched pair is emitted when the second side's row arrives,
// carrying the arriving row's kind; the output is left columns then right columns.
#[test]
fn updating_join_emits_matches_with_arriving_kind() {
    let mut joiner = inner_joiner();
    // Buffer a left row (k=1, v=10); no right yet, so nothing emits.
    assert_eq!(
        joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap().num_rows(),
        0
    );
    // A right row (k=1, v=100) matches it: emit +I (left ++ right).
    let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 0), vec![1]); // left k
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert_eq!(values(&out, 2), vec![1]); // right k
    assert_eq!(values(&out, 3), vec![100]); // right v
    // Retracting the left row emits the matching pair as a retraction.
    let retract = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![3]), true).unwrap();
    assert_eq!(row_kinds(&retract), vec![3]); // -D
    assert_eq!(values(&retract, 1), vec![10]);
    assert_eq!(values(&retract, 3), vec![100]);
}

// A left row matches every buffered right row of its key (cartesian per key); different keys
// never match.
#[test]
fn updating_join_is_cartesian_per_key() {
    let mut joiner = inner_joiner();
    joiner.push(&changelog_join_batch(vec![1, 1, 2], vec![100, 200, 300], vec![0, 0, 0]), false);
    let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap();
    assert_eq!(out.num_rows(), 2); // matches both k=1 right rows, not the k=2 one
    let mut right_vs = values(&out, 3);
    right_vs.sort();
    assert_eq!(right_vs, vec![100, 200]);
}

// A null join key never matches (INNER `a.k = b.k` null semantics): the row is neither joined
// nor stored.
#[test]
fn updating_join_drops_null_keys() {
    let mut joiner = inner_joiner();
    // A right row with a null key, then a left row with a null key — no match either way.
    let right = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![None, Some(1)])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int8Array::from(vec![0, 0])),
        ],
    )
    .unwrap();
    joiner.push(&right, false);
    // Left null key matches nothing; left key=1 matches the stored right (1, 200).
    let left = RecordBatch::try_new(
        right.schema(),
        vec![
            Arc::new(Int64Array::from(vec![None, Some(1)])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int8Array::from(vec![0, 0])),
        ],
    )
    .unwrap();
    let out = joiner.push(&left, true).unwrap();
    assert_eq!(out.num_rows(), 1); // only key=1 pair, not the null-key rows
    assert_eq!(values(&out, 1), vec![20]); // left v
    assert_eq!(values(&out, 3), vec![200]); // right v
}

// The per-side multiset survives a checkpoint, so a post-restore arrival still finds its match.
#[test]
fn updating_join_state_survives_snapshot_restore() {
    let mut joiner = inner_joiner();
    joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false); // buffer right
    let snapshot = joiner.snapshot();
    let mut restored =
        UpdatingJoiner::restore(vec![0], vec![0], JoinKind::Inner, kv_schema(), kv_schema(), None, &snapshot);
    let out = restored.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![10]);
    assert_eq!(values(&out, 3), vec![100]);
}

#[test]
fn updating_join_state_partitions_and_restores_by_flink_key_group() {
    let schema = kv_schema();
    let mut before = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::Inner,
        schema.clone(),
        schema.clone(),
        None,
    );
    before
        .push(&changelog_join_batch(vec![1, 2], vec![10, 20], vec![0, 0]), true)
        .unwrap();
    let groups = before.snapshot_key_groups(128, &[-1]);
    assert!(groups.len() >= 2, "test keys should cover distinct raw key groups");
    let snapshots: Vec<Vec<u8>> = groups
        .iter()
        .map(|&group| before.snapshot_key_group(group, 128, &[-1]))
        .collect();
    let mut restored = UpdatingJoiner::restore_partitions(
        vec![0],
        vec![0],
        JoinKind::Inner,
        schema.clone(),
        schema,
        None,
        &snapshots,
    );
    let out = restored
        .push(&changelog_join_batch(vec![1, 2], vec![100, 200], vec![0, 0]), false)
        .unwrap();
    assert_eq!(values(&out, 0), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![10, 20]);
    assert_eq!(values(&out, 3), vec![100, 200]);
}

// LEFT OUTER: a left row with no right match emits a null-padded row immediately; when a right
// row later matches, the null-pad is retracted (-D) and the matched pair emitted (+I).
#[test]
fn updating_join_left_outer_null_pads_then_retracts() {
    let mut joiner =
        UpdatingJoiner::new(vec![0], vec![0], JoinKind::LeftOuter, kv_schema(), kv_schema(), None);
    // Left row k=1, v=10: no right match → +I[left + null].
    let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert!(out.column(3).is_null(0)); // right v nulled
    // Right row k=1, v=100 arrives: -D[left + null], +I[left + right].
    let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false).unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    assert!(out.column(3).is_null(0)); // the retracted null-pad's right v
    assert!(!out.column(3).is_null(1)); // the matched pair's right v is present
    assert_eq!(values(&out, 1), vec![10, 10]); // both rows carry the left v
}

// LEFT OUTER on a left key that never matches: the null-pad is emitted once and retracted when
// the left row is deleted — net materialized result is empty.
#[test]
fn updating_join_left_outer_unmatched_retract() {
    let mut joiner =
        UpdatingJoiner::new(vec![0], vec![0], JoinKind::LeftOuter, kv_schema(), kv_schema(), None);
    let out = joiner.push(&changelog_join_batch(vec![7], vec![70], vec![0]), true).unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I[left + null]
    let out = joiner.push(&changelog_join_batch(vec![7], vec![70], vec![3]), true).unwrap();
    assert_eq!(row_kinds(&out), vec![3]); // -D[left + null]
    assert!(out.column(3).is_null(0));
}

// SEMI: a left row is emitted once it has a right match; ANTI would emit it while unmatched.
#[test]
fn updating_join_semi_emits_on_match() {
    let mut joiner = UpdatingJoiner::new(vec![0], vec![0], JoinKind::Semi, kv_schema(), kv_schema(), None);
    // Left row with no right match → nothing (semi).
    assert_eq!(joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap().num_rows(), 0);
    // Right row arrives → emit the left row (+I), one column-set (left only).
    let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(out.num_columns(), 3); // left k, left v, $row_kind$ (no right columns)
    assert_eq!(values(&out, 1), vec![10]);
}

// ANTI: a left row is emitted while it has no match, and retracted (-D) once a match arrives.
#[test]
fn updating_join_anti_retracts_on_match() {
    let mut joiner = UpdatingJoiner::new(vec![0], vec![0], JoinKind::Anti, kv_schema(), kv_schema(), None);
    let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I[left] (no match yet)
    let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false).unwrap();
    assert_eq!(row_kinds(&out), vec![3]); // -D[left] (now matched)
    assert_eq!(values(&out, 1), vec![10]);
}

// The `[k, v, rt]` data schema (rt an i64 millis column) both sides carry in the temporal-join
// tests; `rt_to_millis` reads an i64 rowtime directly.
fn temporal_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Int64, false),
    ]))
}

fn temporal_probe_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        temporal_schema(),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(rt)),
        ],
    )
    .unwrap()
}

fn temporal_build_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Int64, false),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(rt)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

fn temporal_joiner(join_type: JoinKind) -> TemporalJoiner {
    TemporalJoiner::new(vec![0], vec![0], 2, 2, join_type, temporal_schema(), temporal_schema(), None)
}

// Each probe row joins the build version valid at its rowtime — the latest accumulate version
// whose rightTime <= the probe time; emission is gated on the watermark.
#[test]
fn temporal_join_picks_version_valid_at_probe_time() {
    let mut joiner = temporal_joiner(JoinKind::Inner);
    // key 1: rate 10@100 then rate 20@300 (+U); key 2: rate 99@100.
    joiner.push_right(&temporal_build_batch(vec![1], vec![10], vec![100], vec![0]));
    joiner.push_right(&temporal_build_batch(vec![1], vec![20], vec![300], vec![2]));
    joiner.push_right(&temporal_build_batch(vec![2], vec![99], vec![100], vec![0]));
    joiner.push_left(&temporal_probe_batch(vec![1, 1, 2], vec![1, 2, 3], vec![200, 500, 150]));
    let out = joiner.advance(i64::MAX);
    assert_eq!(out.num_rows(), 3);
    // probe@200 -> 10, probe@500 -> 20 (the +U version), probe@150 -> 99 (cross-key order varies).
    let mut right_rate = values(&out, 4);
    right_rate.sort();
    assert_eq!(right_rate, vec![10, 20, 99]);
}

// A LEFT temporal join null-pads a probe row whose valid version is missing or a delete marker.
#[test]
fn temporal_join_left_pads_on_delete_or_missing() {
    let mut joiner = temporal_joiner(JoinKind::LeftOuter);
    joiner.push_right(&temporal_build_batch(vec![1], vec![10], vec![100], vec![0]));
    joiner.push_right(&temporal_build_batch(vec![2], vec![99], vec![100], vec![0]));
    joiner.push_right(&temporal_build_batch(vec![2], vec![99], vec![400], vec![3])); // delete @400
    joiner.push_left(&temporal_probe_batch(
        vec![1, 2, 1],
        vec![1, 2, 3],
        vec![50, 500, 200], // 50: before any version; 500: after key-2 delete; 200: -> 10
    ));
    let out = joiner.advance(i64::MAX);
    assert_eq!(out.num_rows(), 3);
    // Exactly one row matched (right rate present); the other two are null-padded.
    let matched = (0..out.num_rows()).filter(|&i| !out.column(4).is_null(i)).count();
    assert_eq!(matched, 1);
}

// A probe row buffered below the watermark stays until a later watermark passes its time, and then
// resolves against a version that arrived in the meantime; state survives a checkpoint.
#[test]
fn temporal_join_buffers_and_survives_snapshot_restore() {
    let mut joiner = temporal_joiner(JoinKind::Inner);
    joiner.push_right(&temporal_build_batch(vec![1], vec![10], vec![100], vec![0]));
    joiner.push_left(&temporal_probe_batch(vec![1], vec![1], vec![500]));
    assert_eq!(joiner.advance(200).num_rows(), 0); // watermark 200 < probe time 500
    let snapshot = joiner.snapshot();
    let mut restored = TemporalJoiner::restore(
        vec![0], vec![0], 2, 2, JoinKind::Inner, temporal_schema(), temporal_schema(), None,
        &snapshot,
    );
    restored.push_right(&temporal_build_batch(vec![1], vec![20], vec![300], vec![2])); // +U @300
    let out = restored.advance(i64::MAX);
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![20]); // resolves to the version valid at 500 (rate 20 @300)
}

#[test]
fn temporal_join_state_partitions_and_restores_by_flink_key_group() {
    let mut before = temporal_joiner(JoinKind::Inner);
    let _ = before.push_right(&temporal_build_batch(vec![1, 2], vec![10, 20], vec![100, 100], vec![0, 0]));
    let _ = before.push_left(&temporal_probe_batch(vec![1, 2], vec![1, 2], vec![500, 500]));
    let groups = before.snapshot_key_groups(128, &[-1]);
    assert!(groups.len() >= 2, "test keys should cover distinct raw key groups");
    let snapshots: Vec<Vec<u8>> = groups
        .iter()
        .map(|&group| before.snapshot_key_group(group, 128, &[-1]))
        .collect();

    let mut restored = TemporalJoiner::restore_partitions(
        vec![0],
        vec![0],
        2,
        2,
        JoinKind::Inner,
        temporal_schema(),
        temporal_schema(),
        None,
        &snapshots,
    );
    let out = restored.advance(i64::MAX);
    let mut rates = values(&out, 4);
    rates.sort_unstable();
    assert_eq!(rates, vec![10, 20]);
}

// A residual non-equi predicate gates the version match: the version valid at the probe time is
// joined only when the pair also satisfies the predicate, else (INNER) the probe row is dropped.
// Joined row is [lk, lamount, lrt, rk, rrate, rrt] = indices [0..6]; predicate is amount > rate.
#[test]
fn temporal_join_applies_non_equi_predicate() {
    let predicate = JoinPredicate {
        kinds: vec![6, 0, 0],     // CALL(>), input_ref, input_ref
        payload: vec![10, 1, 4],  // op GREATER_THAN; probe.amount (col 1) > build.rate (col 4)
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let mut joiner = TemporalJoiner::new(
        vec![0], vec![0], 2, 2, JoinKind::Inner, temporal_schema(), temporal_schema(),
        Some(predicate),
    );
    // key 1: rate 5@100 then rate 50@300 (+U).
    joiner.push_right(&temporal_build_batch(vec![1], vec![5], vec![100], vec![0]));
    joiner.push_right(&temporal_build_batch(vec![1], vec![50], vec![300], vec![2]));
    // amount 10 @200 -> version rate 5, 10 > 5 matches; amount 10 @500 -> version rate 50, fails.
    joiner.push_left(&temporal_probe_batch(vec![1, 1], vec![10, 10], vec![200, 500]));
    let out = joiner.advance(i64::MAX);
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![5]); // only the pair passing amount > rate
}

// A residual non-equi predicate gates which same-key pairs are matches. `left.v > right.v`
// (cols [k, lv, k0, rv] = indices [0,1,2,3]) over an INNER join: of two buffered right rows only
// the one whose v is below the left's v matches.
#[test]
fn updating_join_applies_non_equi_predicate() {
    let predicate = JoinPredicate {
        kinds: vec![6, 0, 0],      // CALL(>), input_ref, input_ref
        payload: vec![10, 1, 3],   // op GREATER_THAN; left.v (col 1) > right.v (col 3)
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::Inner,
        kv_schema(),
        kv_schema(),
        Some(predicate),
    );
    // Buffer two right rows for k=1: v=5 and v=20.
    joiner.push(&changelog_join_batch(vec![1, 1], vec![5, 20], vec![0, 0]), false);
    // Left row k=1, v=10 → matches only the right v=5 (10 > 5), not v=20.
    let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 3), vec![5]); // the one right row passing left.v > right.v
}

// The degree survives a checkpoint: a restored LEFT OUTER joiner still retracts the null-pad when
// the first match arrives post-restore.
#[test]
fn updating_join_outer_degree_survives_snapshot_restore() {
    let mut joiner =
        UpdatingJoiner::new(vec![0], vec![0], JoinKind::LeftOuter, kv_schema(), kv_schema(), None);
    joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true); // +I[left+null], degree 0
    let snapshot = joiner.snapshot();
    let mut restored = UpdatingJoiner::restore(
        vec![0],
        vec![0],
        JoinKind::LeftOuter,
        kv_schema(),
        kv_schema(),
        None,
        &snapshot,
    );
    let out = restored.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false).unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]); // -D[left+null], +I[left+right]
}

// The `[k, v, rt]` data schema both sides of the interval-join tests carry.
fn interval_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Int64, false),
    ]))
}

// An INNER interval joiner over the `[k, v, rt]` schema (key col 0, rowtime col 2).
fn inner_interval_joiner(lower: i64, upper: i64) -> IntervalJoiner {
    IntervalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        lower,
        upper,
        None,
        JoinKind::Inner,
        interval_schema(),
        interval_schema(),
    )
}

// A `[k, v, rt]` batch with int64 rowtime (epoch millis) for the interval-join tests.
fn join_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(interval_schema(), vec![
        Arc::new(Int64Array::from(k)),
        Arc::new(Int64Array::from(v)),
        Arc::new(Int64Array::from(rt)),
    ])
    .unwrap()
}

// INNER interval join: a left row matches a buffered right row of the same key whose rowtime is
// within [rt + lower, rt + upper]; output columns are left ++ right.
#[test]
fn interval_join_emits_matched_pairs() {
    // a.rt BETWEEN b.rt - 1000 AND b.rt + 1000, single equi-key on column 0, rt is column 2.
    let mut joiner = inner_interval_joiner(-1000, 1000);
    // Buffer two right rows for key 1 (rt 5500 in range of left 5000, rt 7000 out of range).
    assert_eq!(joiner.push_right(join_batch(vec![1, 1], vec![100, 200], vec![5500, 7000]), None).unwrap().num_rows(), 0);
    // A left row (k=1, rt=5000): matches the rt=5500 right row only (delta -500 in [-1000,1000]).
    let out = joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 0), vec![1]); // left k
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert_eq!(values(&out, 2), vec![5000]); // left rt
    assert_eq!(values(&out, 3), vec![1]); // right k
    assert_eq!(values(&out, 4), vec![100]); // right v
    assert_eq!(values(&out, 5), vec![5500]); // right rt
}

// Different keys never match, and a pair is emitted once — when its second side arrives —
// regardless of which side arrived first.
#[test]
fn interval_join_matches_on_key_and_emits_once() {
    let mut joiner = inner_interval_joiner(-1000, 1000);
    // Left first: buffer a left row, no right yet.
    assert_eq!(joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None).unwrap().num_rows(), 0);
    // A right row with a different key does not match.
    assert_eq!(joiner.push_right(join_batch(vec![2], vec![100], vec![5000]), None).unwrap().num_rows(), 0);
    // A matching right row emits the pair exactly once.
    let out = joiner.push_right(join_batch(vec![1], vec![100], vec![5500]), None).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![10]);
    assert_eq!(values(&out, 4), vec![100]);
}

// The watermark evicts rows past their last useful rowtime, so a later arrival can no longer
// match an evicted row.
#[test]
fn interval_join_evicts_dead_rows_on_watermark() {
    let mut joiner = inner_interval_joiner(-1000, 1000);
    joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    // Watermark 6000: left.rt - lower = 5000 - (-1000) = 6000, not > 6000, so the row is evicted.
    joiner.advance(6000);
    // A right row that would otherwise match (delta -500) finds nothing buffered.
    assert_eq!(joiner.push_right(join_batch(vec![1], vec![100], vec![5500]), None).unwrap().num_rows(), 0);
}

// The `[k, v, window_start, window_end]` data schema the window-join tests carry.
fn window_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("window_start", DataType::Int64, false),
        Field::new("window_end", DataType::Int64, false),
    ]))
}

// A window joiner of the given kind (key col 0, window bounds cols 2/3) over `window_schema`.
fn window_joiner(kind: JoinKind) -> WindowJoiner {
    WindowJoiner::new(vec![0], vec![0], 2, 3, 2, 3, None, kind, window_schema(), window_schema())
}

// A `[k, v, window_start, window_end]` batch (window bounds as int64 millis) for window-join tests.
fn window_batch(k: Vec<i64>, v: Vec<i64>, ws: Vec<i64>, we: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(window_schema(), vec![
        Arc::new(Int64Array::from(k)),
        Arc::new(Int64Array::from(v)),
        Arc::new(Int64Array::from(ws)),
        Arc::new(Int64Array::from(we)),
    ])
    .unwrap()
}

// The matched (left v, right v) pairs of a join output, sorted (the hash join does not promise
// an output order; parity is over the result set).
fn left_right_values(batch: &RecordBatch) -> Vec<(i64, i64)> {
    let mut pairs: Vec<(i64, i64)> =
        values(batch, 1).into_iter().zip(values(batch, 5)).collect();
    pairs.sort_unstable();
    pairs
}

// INNER window join: left and right rows of the same key in the same window join (their cross
// product) once the watermark closes the window; other windows/keys do not match.
#[test]
fn window_join_emits_matches_when_window_closes() {
    // keys col 0; window_start col 2, window_end col 3 on both sides.
    let mut joiner = window_joiner(JoinKind::Inner);
    // Window [0,1000): left k=1 (two rows) and k=2; right k=1 and k=3.
    joiner.push_left(window_batch(vec![1, 1, 2], vec![10, 11, 20], vec![0, 0, 0], vec![1000, 1000, 1000]));
    joiner.push_right(window_batch(vec![1, 3], vec![100, 300], vec![0, 0], vec![1000, 1000]));
    // A later window [1000,2000) for k=1 on both sides (should not mix with [0,1000)).
    joiner.push_left(window_batch(vec![1], vec![40], vec![1000], vec![2000]));
    joiner.push_right(window_batch(vec![1], vec![400], vec![1000], vec![2000]));

    // Watermark 1000 closes only [0,1000): k=1 matches (2 left × 1 right = 2 rows), k=2/k=3 don't.
    let out = joiner.flush(1000).expect("window join flush");
    assert_eq!(left_right_values(&out), vec![(10, 100), (11, 100)]);

    // Watermark 2000 closes [1000,2000): k=1 matches once.
    let rest = joiner.flush(2000).expect("window join flush");
    assert_eq!(left_right_values(&rest), vec![(40, 400)]);
}

// Buffered window-join rows survive a snapshot/restore round trip.
#[test]
fn window_join_restores_buffered_rows() {
    let mut joiner = window_joiner(JoinKind::Inner);
    joiner.push_left(window_batch(vec![1], vec![10], vec![0], vec![1000]));
    joiner.push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]));
    let snapshot = joiner.snapshot();
    let mut restored = WindowJoiner::restore(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        window_schema(),
        window_schema(),
        &snapshot,
    );
    let out = restored.flush(1000).expect("window join flush");
    assert_eq!(left_right_values(&out), vec![(10, 100)]);
}

#[test]
fn window_join_state_partitions_and_restores_by_flink_key_group() {
    let mut before = window_joiner(JoinKind::Inner);
    let _ = before.push_left(window_batch(vec![1, 2], vec![10, 20], vec![0, 0], vec![1000, 1000]));
    let _ = before.push_right(window_batch(
        vec![1, 2],
        vec![100, 200],
        vec![0, 0],
        vec![1000, 1000],
    ));
    let groups = before.snapshot_key_groups(128, &[-1]);
    assert!(groups.len() >= 2, "test keys should cover distinct raw key groups");
    let snapshots: Vec<Vec<u8>> = groups
        .iter()
        .map(|&group| before.snapshot_key_group(group, 128, &[-1]))
        .collect();

    let mut restored = WindowJoiner::restore_partitions(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        window_schema(),
        window_schema(),
        &snapshots,
    );
    let out = restored.flush(1000).unwrap();
    assert_eq!(left_right_values(&out), vec![(10, 100), (20, 200)]);
}

// LEFT window join: a left row whose window has no matching right row is null-padded when the
// window closes (append-only — emitted once at flush).
#[test]
fn window_left_join_null_pads_unmatched() {
    let mut joiner = window_joiner(JoinKind::LeftOuter);
    // Window [0,1000): left k=1 (matches right) and k=2 (no right match); right k=1 only.
    joiner.push_left(window_batch(vec![1, 2], vec![10, 20], vec![0, 0], vec![1000, 1000]));
    joiner.push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]));
    let out = joiner.flush(1000).expect("window join flush");
    // k=1 emits the matched pair [10,100]; k=2 emits [20, null].
    assert_eq!(out.num_rows(), 2);
    let mut left_vs = values(&out, 1);
    left_vs.sort_unstable();
    assert_eq!(left_vs, vec![10, 20]);
    // Exactly one row (k=2) has a null right v (column 5).
    let null_right = (0..out.num_rows()).filter(|&i| out.column(5).is_null(i)).count();
    assert_eq!(null_right, 1);
}

// Buffered rows survive a snapshot/restore round trip and still match afterward.
#[test]
fn interval_join_restores_buffered_rows() {
    let mut joiner = inner_interval_joiner(-1000, 1000);
    joiner.push_right(join_batch(vec![1], vec![100], vec![5500]), None);
    let snapshot = joiner.snapshot();
    let mut restored = IntervalJoiner::restore(
        vec![0],
        vec![0],
        2,
        2,
        -1000,
        1000,
        None,
        JoinKind::Inner,
        interval_schema(),
        interval_schema(),
        &snapshot,
    );
    let out = restored.push_left(join_batch(vec![1], vec![10], vec![5000]), None).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![100]);
}

fn left_interval_joiner(lower: i64, upper: i64) -> IntervalJoiner {
    IntervalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        lower,
        upper,
        None,
        JoinKind::LeftOuter,
        interval_schema(),
        interval_schema(),
    )
}

// LEFT interval join: a left row that never matches is null-padded once its interval is evicted by
// the watermark (append-only — emitted once). A left row evicts when `rt - lower <= watermark`.
#[test]
fn interval_left_join_null_pads_unmatched_on_eviction() {
    let mut joiner = left_interval_joiner(-1000, 1000);
    // Left row k=1, v=10, rt=5000; no right buffered → no immediate match.
    assert_eq!(joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None).unwrap().num_rows(), 0);
    // Watermark below the eviction point: not yet evicted, nothing emitted.
    assert_eq!(joiner.advance(5000).num_rows(), 0);
    // Watermark at/above 5000 - (-1000) = 6000: the left row is evicted unmatched → [left+null]
    // (append-only, so no $row_kind$ column — just the padded row).
    let out = joiner.advance(6000);
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert!(out.column(3).is_null(0)); // right k nulled
    assert!(out.column(4).is_null(0)); // right v nulled
}

// LEFT interval join: a left row that matches a right row is emitted as a pair and not
// null-padded at eviction.
#[test]
fn interval_left_join_matched_row_not_padded() {
    let mut joiner = left_interval_joiner(-1000, 1000);
    joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    // Right row k=1, rt=5000 within [rt-1000, rt+1000] of the left → emits the matched pair.
    let out = joiner.push_right(join_batch(vec![1], vec![100], vec![5000]), None).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![100]);
    // Evict the left row: it matched, so no null-pad.
    assert_eq!(joiner.advance(10000).num_rows(), 0);
}

// The match flags survive a checkpoint: a restored LEFT interval joiner does not re-pad a left
// row that matched before the snapshot.
#[test]
fn interval_left_join_match_flags_survive_restore() {
    let mut joiner = left_interval_joiner(-1000, 1000);
    joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    joiner.push_right(join_batch(vec![1], vec![100], vec![5000]), None); // marks the left row matched
    let snapshot = joiner.snapshot();
    let mut restored = IntervalJoiner::restore(
        vec![0],
        vec![0],
        2,
        2,
        -1000,
        1000,
        None,
        JoinKind::LeftOuter,
        interval_schema(),
        interval_schema(),
        &snapshot,
    );
    // Evicting the (matched) left row post-restore must emit no null-pad.
    assert_eq!(restored.advance(10000).num_rows(), 0);
}

// Raw keyed state can merge key groups that originated on different subtasks. Outer-join row ids
// are subtask-local, so two such groups may each contain id zero; their matched flags must remain
// attached to the row from their original key group after restore.
#[test]
fn interval_outer_raw_state_remaps_subtask_local_row_ids() {
    let mut matched = left_interval_joiner(-1000, 1000);
    let _ = matched.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    let _ = matched.push_right(join_batch(vec![1], vec![100], vec![5000]), None);
    let matched_groups = matched.snapshot_key_groups(128, &[-1]);
    assert_eq!(matched_groups.len(), 1);

    let mut unmatched = left_interval_joiner(-1000, 1000);
    let _ = unmatched.push_left(join_batch(vec![2], vec![20], vec![5000]), None);
    let unmatched_groups = unmatched.snapshot_key_groups(128, &[-1]);
    assert_eq!(unmatched_groups.len(), 1);
    assert_ne!(matched_groups, unmatched_groups, "test keys need distinct raw key groups");

    let snapshots = vec![
        matched.snapshot_key_group(matched_groups[0], 128, &[-1]),
        unmatched.snapshot_key_group(unmatched_groups[0], 128, &[-1]),
    ];
    let mut restored = IntervalJoiner::restore_partitions(
        vec![0],
        vec![0],
        2,
        2,
        -1000,
        1000,
        None,
        JoinKind::LeftOuter,
        interval_schema(),
        interval_schema(),
        &snapshots,
    );
    let out = restored.advance(10_000);
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 0), vec![2]);
    assert_eq!(values(&out, 1), vec![20]);
    assert!(out.column(4).is_null(0));
}

// ROW_NUMBER over (PARTITION BY key0 ORDER BY rt): a per-key counter in rowtime order, surviving
// across update calls (the unbounded frame).
#[test]
fn window_function_row_number_counts_per_key() {
    let batch = |rt: Vec<i64>, key0: Vec<i64>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("rt", DataType::Int64, false),
                Field::new("key0", DataType::Int64, false),
            ])),
            vec![Arc::new(Int64Array::from(rt)), Arc::new(Int64Array::from(key0))],
        )
        .unwrap()
    };
    let mut over = WindowFunctionOver::new(vec![10]); // ROW_NUMBER
    // Out of rowtime order within the batch: ROW_NUMBER follows rowtime, emitted in input order.
    assert_eq!(values(&over.update(&batch(vec![0, 1000, 0], vec![1, 1, 2])), 0), vec![1, 2, 1]);
    // The counter continues per key across calls.
    assert_eq!(values(&over.update(&batch(vec![2000, 1000], vec![1, 2])), 0), vec![3, 2]);
}

// RANK and DENSE_RANK over (ORDER BY rt): tied rowtimes share a rank; RANK leaves gaps after a
// tie (next jumps to the row position), DENSE_RANK does not.
#[test]
fn window_function_rank_and_dense_rank_handle_ties() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rt", DataType::Int64, false),
            Field::new("key0", DataType::Int64, false),
        ])),
        // One key, rowtimes 10, 10 (tie), 20, 30.
        vec![
            Arc::new(Int64Array::from(vec![10i64, 10, 20, 30])),
            Arc::new(Int64Array::from(vec![1i64, 1, 1, 1])),
        ],
    )
    .unwrap();
    let mut rank = WindowFunctionOver::new(vec![11]); // RANK
    assert_eq!(values(&rank.update(&batch), 0), vec![1, 1, 3, 4]);
    let mut dense = WindowFunctionOver::new(vec![12]); // DENSE_RANK
    assert_eq!(values(&dense.update(&batch), 0), vec![1, 1, 2, 3]);
}

// Decoder over the pre-order encoding: CALL gt ( INPUT_REF a , LIT_LONG 5 ).
#[test]
fn filters_column_greater_than_literal() {
    let mut expression = FilterExpression {
        kinds: vec![6, 0, 1],
        payload: vec![10, 0, 0],
        child_counts: vec![2, 0, 0],
        longs: vec![5],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let out = expression.filter(sample_batch());
    assert_eq!(values(&out, 0), vec![6, 9]);
}

// Arithmetic inside the predicate: CALL gt ( CALL plus ( INPUT_REF a , INPUT_REF b ) , LIT 10 ).
#[test]
fn filters_arithmetic_predicate() {
    let mut expression = FilterExpression {
        kinds: vec![6, 6, 0, 0, 1],
        payload: vec![10, 0, 0, 1, 0],
        child_counts: vec![2, 2, 0, 0, 0],
        longs: vec![10],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let out = expression.filter(sample_batch());
    assert_eq!(values(&out, 0), vec![1, 3, 9]);
}

// An int32 literal keeps the arithmetic in int32, so `v * 2` wraps on overflow like the host
// rather than widening: CALL gt ( CALL times ( INPUT_REF v , LIT_INT 2 ) , LIT_INT 50 ).
#[test]
fn integer_arithmetic_wraps_in_declared_width() {
    let v: ArrayRef = Arc::new(Int32Array::from(vec![30i32, 2_000_000_000]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
        vec![v],
    )
    .unwrap();
    let mut expression = FilterExpression {
        kinds: vec![6, 6, 0, 7, 7],
        payload: vec![10, 2, 0, 0, 1],
        child_counts: vec![2, 2, 0, 0, 0],
        longs: vec![2, 50],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let out = expression.filter(batch);
    let kept = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    // 30*2=60 > 50 keeps 30; 2e9*2 overflows int32 to a negative value, excluded.
    assert_eq!(kept.values(), &[30]);
}

// The native sink writes a batch to Parquet; reading it back yields the same rows.
#[test]
#[cfg(feature = "parquet")]
fn writes_and_reads_parquet() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let batch = sample_batch();
    let path = std::env::temp_dir().join("streamfusion_parquet_roundtrip.parquet");
    let path = path.to_str().unwrap();
    write_parquet(&batch, path);

    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut rows = 0usize;
    let mut first = Vec::new();
    for read in reader {
        let read = read.unwrap();
        let column = read.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        first.extend_from_slice(column.values());
        rows += read.num_rows();
    }
    assert_eq!(rows, batch.num_rows());
    assert_eq!(first, values(&batch, 0));
}

fn ab_batch() -> RecordBatch {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        vec![a, b],
    )
    .unwrap()
}

// A Calc with no condition projects computed columns: [a + b, a].
#[test]
fn calc_projects_computed_columns() {
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 0, 0],
        payload: vec![0, 0, 1, 0], // CALL(+), col a, col b; col a
        child_counts: vec![2, 0, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![0, 3],
        condition_root: -1,
        output_names: vec!["sum".to_string(), "a".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(ab_batch());
    assert_eq!(out.schema().field(0).name(), "sum");
    assert_eq!(out.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(), &[11, 22, 33]);
    assert_eq!(out.column(1).as_any().downcast_ref::<Int64Array>().unwrap().values(), &[1, 2, 3]);
}

// A Calc filters by the condition (a > 2), then projects the survivors.
#[test]
fn calc_filters_then_projects() {
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 1, 0],
        payload: vec![10, 0, 0, 0], // CALL(>), col a, lit; col a
        child_counts: vec![2, 0, 0, 0],
        longs: vec![2],
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![3],
        condition_root: 0,
        output_names: vec!["a".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(ab_batch());
    assert_eq!(out.num_rows(), 1);
    assert_eq!(out.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(), &[3]);
}

// A Calc projects a field pulled out of a ROW/struct column (kind 13 → get_field), the Nexmark
// view shape (`bid.price`).
#[test]
fn calc_extracts_struct_field() {
    let auction: ArrayRef = Arc::new(Int64Array::from(vec![100, 101, 102]));
    let price: ArrayRef = Arc::new(Int64Array::from(vec![99, 40, 200]));
    let bid = StructArray::from(vec![
        (Arc::new(Field::new("auction", DataType::Int64, true)), auction),
        (Arc::new(Field::new("price", DataType::Int64, true)), price),
    ]);
    let et: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 2]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("event_type", DataType::Int64, true),
            Field::new("bid", bid.data_type().clone(), true),
        ])),
        vec![et, Arc::new(bid)],
    )
    .unwrap();

    let mut calc = CalcExpression {
        kinds: vec![13, 0],         // FIELD_ACCESS("price"), col bid
        payload: vec![0, 1],        // strings[0]="price"; bid is column 1
        child_counts: vec![1, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![Some("price".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["price".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    assert_eq!(out.schema().field(0).name(), "price");
    assert_eq!(
        out.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(),
        &[99, 40, 200]
    );
}

// A Calc projects SQL subscripts (kind 19 → ITEM): `nums[1]` over an ARRAY column and
// `tags['a']` over a MAP column, both NULL for an empty/null collection or an absent key.
#[test]
fn calc_subscripts_array_and_map() {
    use arrow::array::{Int64Builder, MapBuilder};
    let nums = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
        Some(vec![Some(10), Some(20)]),
        Some(vec![]),
        None,
    ]);
    let mut tags = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    tags.keys().append_value("a");
    tags.values().append_value(5);
    tags.keys().append_value("b");
    tags.values().append_value(6);
    tags.append(true).unwrap();
    tags.keys().append_value("b");
    tags.values().append_value(7);
    tags.append(true).unwrap();
    tags.append(false).unwrap();
    let tags = tags.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("nums", nums.data_type().clone(), true),
            Field::new("tags", tags.data_type().clone(), true),
        ])),
        vec![Arc::new(nums), Arc::new(tags)],
    )
    .unwrap();

    let mut calc = CalcExpression {
        // Root 0: ITEM(col nums, lit-int 1); root 1: ITEM(col tags, lit-string "a").
        kinds: vec![19, 0, 7, 19, 0, 3],
        payload: vec![0, 0, 0, 0, 1, 0],
        child_counts: vec![2, 0, 0, 2, 0, 0],
        longs: vec![1],
        doubles: vec![],
        strings: vec![Some("a".to_string())],
        projection_roots: vec![0, 3],
        condition_root: -1,
        output_names: vec!["first_num".to_string(), "tag_a".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let first_num = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(first_num.value(0), 10);
    assert!(first_num.is_null(1) && first_num.is_null(2));
    let tag_a = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(tag_a.value(0), 5);
    assert!(tag_a.is_null(1) && tag_a.is_null(2));
}

// A Calc projecting a mixed-case top-level column (INPUT_REF) must resolve it by its exact name;
// `col()` would lower-case "dateTime" to "datetime" and fail to compile (the Nexmark q0/q1 rowtime).
#[test]
fn calc_projects_mixed_case_column() {
    let value: ArrayRef = Arc::new(Int64Array::from(vec![5, 7, 9]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("dateTime", DataType::Int64, true)])),
        vec![value],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![0],
        payload: vec![0],
        child_counts: vec![0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["dateTime".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    assert_eq!(out.schema().field(0).name(), "dateTime");
    assert_eq!(
        out.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(),
        &[5, 7, 9]
    );
}

// SPLIT_INDEX(url, '/', 3) over the Calc path: 0-based whole-separator split, NULL out of range /
// for an empty input / for a null argument (Flink's splitByWholeSeparatorPreserveAllTokens).
#[test]
fn calc_split_index_matches_flink() {
    let url: ArrayRef = Arc::new(StringArray::from(vec![
        Some("http://h/a/b"),
        Some("x"),
        Some(""),
        None,
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("url", DataType::Utf8, true)])),
        vec![url],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3, 7],    // CALL(SPLIT_INDEX), col url, lit "/", lit 3
        payload: vec![85, 0, 0, 0], // op 85; col 0; strings[0]; longs[0]
        child_counts: vec![3, 0, 0, 0],
        longs: vec![3],
        doubles: vec![],
        strings: vec![Some("/".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["dir".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(col.value(0), "a"); // ["http:","","h","a","b"][3]
    assert!(col.is_null(1)); // ["x"] has no index 3
    assert!(col.is_null(2)); // empty input -> no tokens
    assert!(col.is_null(3)); // null url
}

// DATE_FORMAT(ts, '%Y-%m-%d') over the Calc path: formats the timestamp's UTC wall-clock, NULL for
// a null input (the JVM encoder supplies the chrono pattern).
#[test]
fn calc_date_format_matches_flink() {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
        Some(0),
        Some(86_400_000),
        None,
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )])),
        vec![ts],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3],       // CALL(DATE_FORMAT), col ts, lit "%Y-%m-%d"
        payload: vec![86, 0, 0],    // op 86; col 0; strings[0]
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![Some("%Y-%m-%d".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["d".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(col.value(0), "1970-01-01");
    assert_eq!(col.value(1), "1970-01-02");
    assert!(col.is_null(2));
}

// EXTRACT(HOUR FROM ts) over the Calc path (q14's HOUR): the integer field of the timestamp's UTC
// wall-clock, NULL for a null input. epoch 0 = 1970-01-01T00:00 (hour 0); 86_400_000 + 3_600_000 =
// 1970-01-02T01:00 (hour 1).
#[test]
fn calc_extract_hour_matches_flink() {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
        Some(0),
        Some(86_400_000 + 3_600_000),
        None,
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )])),
        vec![ts],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3],    // CALL(EXTRACT), col ts, lit "hour"
        payload: vec![89, 0, 0], // op 89; col 0; strings[0]
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![Some("hour".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["h".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(col.value(0), 0);
    assert_eq!(col.value(1), 1);
    assert!(col.is_null(2));
}

#[test]
fn calc_regexp_extract_matches_flink() {
    let url: ArrayRef = Arc::new(StringArray::from(vec![
        Some("channel_id=apple&x=1"),      // matches at ^, group 2 = "apple"
        Some("https://h?a=1&channel_id=9"), // matches after &, group 2 = "9"
        Some("no channel here"),            // no match -> NULL
        None,                               // null input -> NULL
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("url", DataType::Utf8, true)])),
        vec![url],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3, 7],    // CALL(REGEXP_EXTRACT), col url, lit pattern, lit 2
        payload: vec![88, 0, 0, 0], // op 88; col 0; strings[0]; longs[0]
        child_counts: vec![3, 0, 0, 0],
        longs: vec![2],
        doubles: vec![],
        strings: vec![Some("(&|^)channel_id=([^&]*)".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["channel_id".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(col.value(0), "apple");
    assert_eq!(col.value(1), "9");
    assert!(col.is_null(2));
    assert!(col.is_null(3));
}

// TIMESTAMP - INTERVAL arithmetic (q7's join residual): a day-time interval literal subtracted
// from a timestamp yields a timestamp (millis - millis), NULL for a null input.
#[test]
fn calc_timestamp_minus_interval() {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![Some(10_000), None]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )])),
        vec![ts],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 15],   // CALL(MINUS), col ts, INTERVAL literal
        payload: vec![1, 0, 0],  // op 1 (MINUS); col 0; longs[0]
        child_counts: vec![2, 0, 0],
        longs: vec![5_000], // 5 seconds
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["earlier".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col =
        out.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
    assert_eq!(col.value(0), 5_000); // 10s - 5s
    assert!(col.is_null(1));
}

// The by-key split sends every row with the same key to the same partition and preserves all
// rows, for any partition count.
#[test]
fn partitions_a_batch_by_key() {
    use std::collections::HashMap;
    let n = 1000usize;
    let key: ArrayRef = Arc::new(Int64Array::from((0..n as i64).map(|i| i % 37).collect::<Vec<_>>()));
    let value: ArrayRef = Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![key, value],
    )
    .unwrap();

    for num_partitions in [1usize, 3, 8] {
        let parts = partition_batch(&batch, &[0], &[-1], num_partitions, num_partitions);
        let mut rows = 0usize;
        let mut key_to_partition: HashMap<i64, usize> = HashMap::default();
        for (partition, sub) in &parts {
            assert!(*partition < num_partitions);
            let keys = sub.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..sub.num_rows() {
                // Each key is consistently assigned to one partition.
                let prev = key_to_partition.insert(keys.value(i), *partition);
                if let Some(p) = prev {
                    assert_eq!(p, *partition, "key {} split across partitions", keys.value(i));
                }
            }
            rows += sub.num_rows();
        }
        assert_eq!(rows, n, "all rows preserved for {num_partitions} partitions");
    }
}

// The compiled predicate is cached after the first batch and reused.
#[test]
fn compiles_once_and_reuses() {
    let mut expression = FilterExpression {
        kinds: vec![6, 0, 1],
        payload: vec![12, 0, 0],
        child_counts: vec![2, 0, 0],
        longs: vec![5],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let first = expression.filter(sample_batch());
    assert!(expression.compiled.is_some());
    let second = expression.filter(sample_batch());
    assert_eq!(values(&first, 0), values(&second, 0));
    assert_eq!(values(&first, 0), vec![1, 3]);
}

// The digit-writing fast plan renders byte-identically to chrono's own formatter across the
// admitted patterns, including 4-digit-year edges the plan pads (year 500) and the out-of-range
// years it must hand back to chrono.
#[test]
fn date_format_fast_plan_matches_chrono() {
    use std::fmt::Write as _;
    let patterns = ["%Y-%m-%d", "%H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"];
    let millis: Vec<i64> = vec![
        0,
        -1,
        -62_135_596_800_000,     // year 0001
        -46_388_649_600_000,     // year 0500 — zero-padded 4-digit year
        1_700_000_000_123,
        253_402_300_799_999,     // year 9999 upper edge
        253_402_300_800_000,     // year 10000 — must fall back to chrono
    ];
    let mut compiled = CompiledFormat::new();
    let mut buf = String::new();
    for pattern in patterns {
        let items = chrono::format::StrftimeItems::new(pattern).parse_to_owned().expect("pattern");
        for &t in &millis {
            let wall = chrono::DateTime::from_timestamp_millis(t).expect("timestamp").naive_utc();
            compiled.format_into(&mut buf, wall, pattern).expect("format");
            let mut expected = String::new();
            write!(expected, "{}", wall.format_with_items(items.iter())).expect("chrono render");
            assert_eq!(buf, expected, "pattern {pattern} at {t}ms");
        }
    }
}

// The driver-init handshake fills the vtable only for an ABI version this library speaks; anything
// else is refused, which the connector treats as "stay on the JVM-mediated decode".
#[test]
fn format_driver_init_gates_on_version() {
    extern "C" fn sentinel(_: i64, _: i64, _: i64, _: i64, _: i64) -> i32 {
        99
    }
    let mut driver = FormatDriver { decode_body_batch: sentinel };
    assert_ne!(streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_1 + 1, &mut driver), 0);
    assert_eq!(driver.decode_body_batch as usize, sentinel as usize);
    assert_eq!(streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_1, &mut driver), 0);
    assert_ne!(driver.decode_body_batch as usize, sentinel as usize);
    assert_ne!(streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_1, std::ptr::null_mut()), 0);
}
