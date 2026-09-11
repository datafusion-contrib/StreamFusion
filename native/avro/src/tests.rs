use super::*;

fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

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

const DBZ_AVRO_WRITER: &str = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
    {"name":"before","type":["null",{"type":"record","name":"record_before","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"internal","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"after","type":["null",{"type":"record","name":"record_after","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"internal","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"source","type":["null","string"],"default":null},
    {"name":"op","type":"string"},
    {"name":"ts_ms","type":["null","long"],"default":null}]}"#;

const DBZ_AVRO_READER: &str = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
    {"name":"before","type":["null",{"type":"record","name":"record_before","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"after","type":["null",{"type":"record","name":"record_after","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"op","type":["null","string"],"default":null}]}"#;

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

    let out = new_decoder(
        FORMAT_AVRO,
        Arc::new(Schema::empty()),
        reader_schema,
        "",
        0,
        false,
        "",
    )
    .decode(&body);

    assert_eq!(out.num_rows(), 2);
    let id = out
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names = out
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
}

// Confluent Avro (format 1), registry-driven: the store starts empty, writer schemas arrive by id
// (as the JVM fetches them from the schema registry), and each message resolves against the reader
// schema. Covers the two things the single-schema path never exercised: a mid-batch schema-id
// switch (the decoder flushes internally; the flushes concatenate under the one reader shape) and
// a writer record named differently from the reader (the JVM rebuilds the fetched schema onto the
// reader's record names — mirroring Avro Java's lenient name check — and arrow-avro also accepts
// the historical alias form pinned here).
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

    let mut decoder = new_decoder(
        FORMAT_AVRO_CONFLUENT,
        Arc::new(Schema::empty()),
        "",
        reader,
        0,
        false,
        "",
    );
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
    let id = out
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.values(), &[1, 2, 3]);
    let names = out
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        (names.value(0), names.value(1), names.value(2)),
        ("a", "b", "c")
    );
}

fn dbz_zigzag(n: i64) -> Vec<u8> {
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

fn dbz_string(s: &str) -> Vec<u8> {
    let mut v = dbz_zigzag(s.len() as i64);
    v.extend_from_slice(s.as_bytes());
    v
}

/// One fully-populated writer image datum in `DBZ_AVRO_WRITER`'s layout: id, name, the
/// writer-only `internal` field the reader skips, then ts — every field on union branch 1.
fn dbz_image(id: i64, name: &str, ts_millis: i64) -> Vec<u8> {
    let mut v = dbz_zigzag(1);
    v.extend(dbz_zigzag(id));
    v.extend(dbz_zigzag(1));
    v.extend(dbz_string(name));
    v.extend(dbz_zigzag(1));
    v.extend(dbz_string("writer-only"));
    v.extend(dbz_zigzag(1));
    v.extend(dbz_zigzag(ts_millis));
    v
}

/// A Confluent-framed writer envelope datum in `DBZ_AVRO_WRITER`'s field order, with a populated
/// `source` and a null `ts_ms` for the reader to resolve away.
fn dbz_message(
    schema_id: u32,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    op: &str,
) -> Vec<u8> {
    let mut v = vec![0x00];
    v.extend_from_slice(&schema_id.to_be_bytes());
    for image in [before, after] {
        match image {
            None => v.extend(dbz_zigzag(0)),
            Some(bytes) => {
                v.extend(dbz_zigzag(1));
                v.extend(bytes);
            }
        }
    }
    v.extend(dbz_zigzag(1));
    v.extend(dbz_string("dbz-source"));
    v.extend(dbz_string(op));
    v.extend(dbz_zigzag(0));
    v
}

fn dbz_decoder() -> MessageDecoder {
    let physical = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let mut decoder = new_decoder(
        FORMAT_DEBEZIUM_AVRO_CONFLUENT,
        physical,
        "",
        DBZ_AVRO_READER,
        0,
        false,
        "",
    );
    decoder.register_writer_schema(5, DBZ_AVRO_WRITER);
    decoder
}

// The Avro envelope fans out exactly like the JSON dialect — c/r → INSERT from `after`, u →
// UPDATE_BEFORE + UPDATE_AFTER, d → DELETE from `before` — while null and empty bodies (Kafka
// tombstones, which Flink returns on without collecting) contribute no rows, and the image
// payloads land on the boundary column types (the timestamp long reads as epoch millis and scales
// to the boundary's nanoseconds, exactly like a plain avro-confluent column).
#[test]
fn debezium_avro_decode_emits_changelog() {
    let insert = dbz_message(5, None, Some(dbz_image(1, "a", 1_000)), "c");
    let read = dbz_message(5, None, Some(dbz_image(2, "b", 2_000)), "r");
    let update = dbz_message(
        5,
        Some(dbz_image(2, "b", 2_000)),
        Some(dbz_image(2, "b2", 3_000)),
        "u",
    );
    let delete = dbz_message(5, Some(dbz_image(1, "a", 1_000)), None, "d");
    let body = bodies(vec![
        Some(&insert),
        None,
        Some(&read),
        Some(&[]),
        Some(&update),
        Some(&delete),
    ]);

    let out = dbz_decoder().decode(&body);

    assert_eq!(out.num_rows(), 5);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 2, 1]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..5).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "b", "b2", "a"]
    );
    let ts = out
        .column(2)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(
        ts.values(),
        &[
            1_000_000_000,
            2_000_000_000,
            2_000_000_000,
            3_000_000_000,
            1_000_000_000
        ]
    );
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 0, 1, 2, 3]);
    assert_eq!(out.schema().field(3).name(), ROW_KIND_COLUMN);
}

// Schema evolution mid-batch: a second registered writer (aligned the same way) whose images lack
// the `ts` field — the reader default (null) fills it — interleaved with the first writer in one
// batch (the decoder flushes internally on the id switch; the flushes concatenate under the one
// reader shape).
#[test]
fn debezium_avro_decodes_evolving_writer_schemas() {
    let writer_v2 = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
        {"name":"before","type":["null",{"type":"record","name":"record_before","fields":[
            {"name":"id","type":["null","long"],"default":null},
            {"name":"name","type":["null","string"],"default":null}]}],"default":null},
        {"name":"after","type":["null",{"type":"record","name":"record_after","fields":[
            {"name":"id","type":["null","long"],"default":null},
            {"name":"name","type":["null","string"],"default":null}]}],"default":null},
        {"name":"op","type":"string"}]}"#;
    // Writer-v2 envelope: {before, after, op} only, images without ts.
    let v2_message = |before: Option<Vec<u8>>, after: Option<Vec<u8>>, op: &str| {
        let mut v = vec![0x00];
        v.extend_from_slice(&9u32.to_be_bytes());
        for image in [before, after] {
            match image {
                None => v.extend(dbz_zigzag(0)),
                Some(bytes) => {
                    v.extend(dbz_zigzag(1));
                    v.extend(bytes);
                }
            }
        }
        v.extend(dbz_string(op));
        v
    };
    let v2_image = |id: i64, name: &str| {
        let mut v = dbz_zigzag(1);
        v.extend(dbz_zigzag(id));
        v.extend(dbz_zigzag(1));
        v.extend(dbz_string(name));
        v
    };
    let mut decoder = dbz_decoder();
    decoder.register_writer_schema(9, writer_v2);

    let m1 = dbz_message(5, None, Some(dbz_image(1, "a", 1_000)), "c");
    let m2 = v2_message(None, Some(v2_image(2, "b")), "c");
    let m3 = dbz_message(5, Some(dbz_image(1, "a", 1_000)), None, "d");
    let out = decoder.decode(&bodies(vec![Some(&m1), Some(&m2), Some(&m3)]));

    assert_eq!(out.num_rows(), 3);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 1]);
    let ts = out
        .column(2)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(!ts.is_null(0) && ts.is_null(1) && !ts.is_null(2));
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 0, 3]);
}

// A null "before" where the op reads it fails the job (Flink's REPLICA IDENTITY error path — this
// format has no ignore-parse-errors, so there is no skip mode to fall into).
#[test]
#[should_panic(expected = "null \"before\"")]
fn debezium_avro_null_before_update_fails() {
    let update = dbz_message(5, None, Some(dbz_image(2, "b", 2_000)), "u");
    dbz_decoder().decode(&bodies(vec![Some(&update)]));
}

#[test]
#[should_panic(expected = "null \"before\"")]
fn debezium_avro_null_before_delete_fails() {
    let delete = dbz_message(5, None, Some(dbz_image(2, "b", 2_000)), "d");
    dbz_decoder().decode(&bodies(vec![Some(&delete)]));
}

// An unrecognized op fails, matching Flink's IOException on an unknown "op" value.
#[test]
#[should_panic(expected = "unknown CDC operation")]
fn debezium_avro_unknown_op_fails() {
    let unknown = dbz_message(5, None, Some(dbz_image(1, "a", 1_000)), "t");
    dbz_decoder().decode(&bodies(vec![Some(&unknown)]));
}
