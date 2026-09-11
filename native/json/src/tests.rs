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
        Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::Binary,
            true,
        )])),
        vec![column],
    )
    .unwrap()
}

/// Keyed decode: the raw Kafka key composes with the value decode per record — Flink's key/value
/// merge with the raw key format's exactly-one key row. A JSON value fanning a top-level array
/// into N rows repeats the record's key N times; a dropped record (skip mode) contributes nothing;
/// a NULL Kafka key keeps the record with a NULL key column (raw's null-key rule); and the key
/// column position interleaves with the value positions in physical schema order.
#[test]
fn keyed_decode_composes_raw_keys_with_the_value_rows() {
    use arrow::array::{Array, StringArray};

    // Physical schema: [k BIGINT (the key, position 0), id BIGINT, name STRING] — EXCEPT_KEY, so
    // the value decode owns positions 1 and 2.
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let keys: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(7i64.to_be_bytes().as_slice()),
        Some(8i64.to_be_bytes().as_slice()),
        None,
        Some(9i64.to_be_bytes().as_slice()),
    ]));
    let bodies: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(br#"{"id": 1, "name": "a"}"#.as_slice()),
        // A top-level array fans out into two rows sharing record 1's key.
        Some(br#"[{"id": 2, "name": "b"}, {"id": 3, "name": "c"}]"#.as_slice()),
        // A null Kafka key keeps the record: raw decodes it to a NULL key column.
        Some(br#"{"id": 4, "name": "d"}"#.as_slice()),
        // A malformed body drops the whole record in skip mode — key and all.
        Some(b"not json".as_slice()),
    ]));
    let records = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, true),
            Field::new("body", DataType::Binary, true),
        ])),
        vec![keys, bodies],
    )
    .unwrap();

    let decoder = new_decoder(
        FORMAT_JSON,
        physical,
        "",
        "",
        0,
        true,
        "keyed.key-position=0\nkeyed.value-positions=1,2\n",
    );
    let out = decoder.decode(&records);

    assert_eq!(out.num_rows(), 4);
    let k = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((k.value(0), k.value(1), k.value(2)), (7, 8, 8));
    assert!(k.is_null(3), "a null Kafka key must stay a NULL key column");
    let id = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 3, 4]);
    let names = out
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(3), "d");
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

    let out = new_decoder(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    // 1 (insert) + 2 (update) + 1 (delete) physical rows.
    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "b2", "c"]
    );
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5, 3.5, 4.5]);
    // INSERT(0), UPDATE_BEFORE(1), UPDATE_AFTER(2), DELETE(3) — Flink's RowKind byte values.
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 1, 2, 3]);
    assert_eq!(out.schema().field(3).name(), ROW_KIND_COLUMN);
}

// A tombstone (null or zero-length body) is dropped, leaving the valid records — matching Flink,
// which skips them regardless of error handling.
#[test]
fn cdc_debezium_skips_tombstone() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"r"}"#;
    let body = bodies(vec![None, Some(b"".as_slice()), Some(insert.as_slice())]);

    let out = new_decoder(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 1);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0]); // "r" snapshot read → INSERT
}

// Flink's tombstone check is `message.length == 0` — a whitespace-only body reaches Jackson,
// yields no envelope, and the op read NPEs into "Corrupt ... JSON message": a job failure in
// default mode, never a silent skip.
#[test]
#[should_panic(expected = "Corrupt Debezium JSON message ' \n\t '.")]
fn cdc_whitespace_body_is_not_a_tombstone() {
    let body = bodies(vec![Some(b" \n\t ".as_slice())]);
    new_decoder(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

#[test]
#[should_panic(expected = "Corrupt Maxwell JSON message '   '.")]
fn cdc_maxwell_whitespace_body_fails_with_its_dialect_name() {
    let body = bodies(vec![Some(b"   ".as_slice())]);
    new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

// Under ignore-parse-errors the whitespace-only message drops whole (Flink's per-message catch),
// keeping its neighbors.
#[test]
fn cdc_whitespace_body_drops_in_skip_mode() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c"}"#;
    let body = bodies(vec![Some(b"  ".as_slice()), Some(insert.as_slice())]);

    let out = new_decoder(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, true, "").decode(&body);

    assert_eq!(out.num_rows(), 1);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1]);
}

// An unrecognized op fails the decode rather than silently dropping the row — Flink throws on it by
// default, so failing keeps the result identical (the planner routes here only when the table does
// not set ignore-parse-errors, i.e. Flink is in throw mode too).
#[test]
#[should_panic(expected = "unknown CDC operation")]
fn cdc_unknown_op_fails() {
    let unknown = br#"{"before":null,"after":{"id":9,"name":"z","score":9.5},"op":"x"}"#;
    new_decoder(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "")
        .decode(&bodies(vec![Some(unknown.as_slice())]));
}

// A null "before" on an update fails (Flink's REPLICA_IDENTITY error), not a silent drop.
#[test]
#[should_panic(expected = "null \"before\"")]
fn cdc_debezium_null_before_update_fails() {
    let update = br#"{"before":null,"after":{"id":2,"name":"b","score":2.5},"op":"u"}"#;
    new_decoder(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "")
        .decode(&bodies(vec![Some(update.as_slice())]));
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

    let out = new_decoder(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, true, "").decode(&body);

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
    let body = bodies(vec![
        Some(good.as_slice()),
        Some(malformed),
        Some(bad_type),
        Some(also_good),
    ]);

    let out = new_decoder(FORMAT_JSON, json_schema(), "", "", 0, true, "").decode(&body);

    // Flink's JSON ignore-parse-errors granularity (parity-pinned): a structurally bad document
    // drops the whole message, but a bad VALUE nulls just that field and keeps the row.
    assert_eq!(out.num_rows(), 3);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((id.value(0), id.value(2)), (1, 4));
    assert!(id.is_null(1));
    let name = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(1), "c");
}

// Skip mode with nothing to skip takes the batched fast path and decodes everything.
#[test]
fn json_skip_mode_clean_batch_decodes_in_full() {
    let a = br#"{"id":1,"name":"a","score":1.5}"#;
    let b = br#"{"id":2,"name":"b","score":2.5}"#;
    let body = bodies(vec![Some(a.as_slice()), Some(b)]);

    let out = new_decoder(FORMAT_JSON, json_schema(), "", "", 0, true, "").decode(&body);

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

    let out = new_decoder(FORMAT_OGG_JSON, json_schema(), "", "", 0, false, "").decode(&body);

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

    let out = new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    // UPDATE_BEFORE keeps the old name "b"; the unchanged id/score are pulled from `data`.
    assert_eq!(
        (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "b2", "c"]
    );
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
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

    let out = new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(names.is_null(0)); // UPDATE_BEFORE: name was explicitly null before the update
    assert_eq!(names.value(1), "was-null");
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 1]); // absent from `old` → unchanged → copied from `data`
}

// Flink's findValue is a RECURSIVE depth-first search: a physical field name buried inside a
// nested container under `old` counts as present, and since the top-level decode of `old` saw
// nothing there, UPDATE_BEFORE keeps the null — it must NOT copy the post-image value.
#[test]
fn cdc_maxwell_old_presence_descends_nested_containers() {
    use arrow::array::Array;
    let nested_object =
        br#"{"data":{"id":1,"name":"a2","score":1.5},"old":{"junk":{"score":9}},"type":"update"}"#;
    let nested_array =
        br#"{"data":{"id":2,"name":"b2","score":2.5},"old":{"junk":[{"name":"hidden"}]},"type":"update"}"#;
    let body = bodies(vec![Some(nested_object.as_slice()), Some(nested_array)]);

    let out = new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Message 0's UPDATE_BEFORE: id/name absent everywhere in old → copied; score found at
    // old.junk.score → present, keeps the top-level null (not 9, not data's 1.5).
    assert_eq!((id.value(0), names.value(0)), (1, "a2"));
    assert!(scores.is_null(0));
    // Message 1's UPDATE_BEFORE: name found inside an array element → present, kept null.
    assert_eq!((id.value(2), scores.value(2)), (2, 2.5));
    assert!(names.is_null(2));
}

// Jackson's tree build collapses a duplicate key to its LAST occurrence, both for the envelope's
// `old` field and inside it — names reachable only through a discarded earlier subtree are not
// found by findValue.
#[test]
fn cdc_maxwell_duplicate_keys_keep_the_last_occurrence() {
    use arrow::array::Array;
    let duplicate_old =
        br#"{"data":{"id":1,"name":"a2","score":1.5},"old":{"name":"x"},"old":{"score":9},"type":"update"}"#;
    let duplicate_inside =
        br#"{"data":{"id":2,"name":"b2","score":2.5},"old":{"junk":{"score":9},"junk":{"keep":1}},"type":"update"}"#;
    let body = bodies(vec![Some(duplicate_old.as_slice()), Some(duplicate_inside)]);

    let out = new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Message 0: the second `old` wins — name is unchanged (copied from data), score kept from old.
    assert_eq!(names.value(0), "a2");
    assert_eq!(scores.value(0), 9.0);
    // Message 1: the second `junk` wins, discarding the subtree holding score → score copied.
    assert_eq!(scores.value(2), 2.5);
}

// Flink reads `old` unchecked on an update (row.getRow / old.getRow(i)), so a missing `old` or a
// Canal `old` array shorter than `data` is a corrupt message that fails the job — never a silent
// fall-back to the post-image.
#[test]
#[should_panic(expected = "null \"old\"/pre image")]
fn cdc_maxwell_update_without_old_fails() {
    let update = br#"{"data":{"id":1,"name":"x","score":1.5},"type":"update"}"#;
    let body = bodies(vec![Some(update.as_slice())]);
    new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

#[test]
#[should_panic(expected = "\"old\" array is shorter")]
fn cdc_canal_uneven_update_arrays_fail() {
    let update = br#"{"data":[{"id":1,"name":"x","score":1.5},{"id":2,"name":"y","score":2.5}],"old":[{"name":"w"}],"type":"UPDATE"}"#;
    let body = bodies(vec![Some(update.as_slice())]);
    new_decoder(FORMAT_CANAL_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

// Canal's findValue presence scan covers the WHOLE `old` array: a key present in any element
// counts as present for every paired element, exactly as Flink's oldField.findValue over the
// array node behaves.
#[test]
fn cdc_canal_presence_is_per_message_across_elements() {
    let update = br#"{"data":[{"id":1,"name":"a2","score":1.5},{"id":2,"name":"b2","score":2.5}],"old":[{"name":"a"},{"id":2}],"type":"UPDATE"}"#;
    let body = bodies(vec![Some(update.as_slice())]);

    let out = new_decoder(FORMAT_CANAL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4); // two elements, UB+UA each
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
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

    let out = new_decoder(FORMAT_CANAL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    // 2 inserts + (update → UB + UA); CREATE dropped.
    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 3, 3]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "c", "c"]
    );
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
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
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
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

#[test]
fn json_decode_preserves_nested_values_around_null_runs() {
    let nested = DataType::Struct(Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));
    let schema = Arc::new(Schema::new(vec![Field::new("event", nested, true)]));
    let batch = bodies(vec![
        Some(br#"{"event":null}"#),
        Some(br#"{"event":null}"#),
        Some(br#"{"event":{"id":7,"name":"a","tags":["x","y"]}}"#),
        Some(br#"{"event":null}"#),
        Some(br#"{"event":{"id":8,"name":"b","tags":[]}}"#),
        Some(br#"{"event":null}"#),
        Some(br#"{"event":null}"#),
    ]);

    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let events = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(events.len(), 7);
    for row in [0, 1, 3, 5, 6] {
        assert!(events.is_null(row));
    }
    assert!(events.is_valid(2));
    assert!(events.is_valid(4));

    let ids = events
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((ids.value(2), ids.value(4)), (7, 8));
    for row in [0, 1, 3, 5, 6] {
        assert!(ids.is_null(row));
    }

    let tags = events
        .column_by_name("tags")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let first_tags = tags.value(2);
    let first_tags = first_tags.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!((first_tags.value(0), first_tags.value(1)), ("x", "y"));
    assert_eq!(tags.value_length(4), 0);
    for row in [0, 1, 3, 5, 6] {
        assert!(tags.is_null(row));
    }
}

// Flink's deserialize skips only a null or ZERO-LENGTH body before parsing; an all-whitespace
// document reaches Jackson and fails ("no content to map due to end-of-input") — the job dies in
// strict mode, the message drops under ignore-parse-errors. Both native subpaths (simd and the
// decimal-bearing arrow-json route) reproduce that split.
#[test]
fn json_decode_whitespace_only_body_fails_strict_and_drops_lenient() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let empty_and_blank = || {
        bodies(vec![
            Some(b"".as_slice()),
            Some(b" \t\r\n"),
            Some(br#"{"id": 1}"#),
        ])
    };
    let lenient = crate::json::JsonEnv {
        lenient: true,
        ..Default::default()
    };

    let strict = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&empty_and_blank()))).is_err());
    let out = JsonDecoder::new(json_schema(), lenient).decode(&empty_and_blank());
    assert_eq!(out.num_rows(), 1); // empty skipped silently, whitespace dropped, the document kept
    assert_eq!(values(&out, 0), vec![1]);
    // A zero-length body alone is not an error even in strict mode.
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default())
        .decode(&bodies(vec![Some(b"".as_slice())]));
    assert_eq!(out.num_rows(), 0);

    let decimal: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("dec", DataType::Decimal128(5, 2), true),
        Field::new("id", DataType::Int64, true),
    ]));
    let strict = JsonDecoder::new(decimal.clone(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&empty_and_blank()))).is_err());
    let out = JsonDecoder::new(decimal, lenient).decode(&empty_and_blank());
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![1]);
}

// Flink's row converter counts every MATCHED key occurrence — duplicates included — and skips all
// remaining keys once the counter reaches the arity. So under (a, b): {"a":1,"b":2,"a":99} keeps
// a=1 (the duplicate lands after saturation), while {"a":1,"a":2} spends both slots on `a` (last
// wins) and leaves b null. The rule applies at every ROW nesting level.
#[test]
fn json_decode_duplicate_row_keys_saturate_flinks_field_counter() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"a": 1, "b": 2, "a": 99}"#),
        Some(br#"{"a": 1, "a": 2}"#),
        Some(br#"{"a": 1, "a": 2, "b": 5}"#),
        Some(br#"{"x": 0, "a": 1, "x": 0, "a": 2, "b": 7}"#), // unknown keys never count
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let a = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let b = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((a.value(0), b.value(0)), (1, 2));
    assert_eq!(a.value(1), 2);
    assert!(b.is_null(1));
    assert_eq!(a.value(2), 2);
    assert!(b.is_null(2));
    assert_eq!(a.value(3), 2);
    assert!(b.is_null(3));

    let nested: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "r",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        true,
    )]));
    let batch = bodies(vec![Some(br#"{"r": {"a": 1, "b": 2, "a": 99}}"#)]);
    let out = JsonDecoder::new(nested, crate::json::JsonEnv::default()).decode(&batch);
    let r = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let a = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let b = r.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((a.value(0), b.value(0)), (1, 2));
}

// Nexmark normally writes fields in schema order, but the ordered-dispatch fast path must retain
// the compiled lookup for reordered and unknown keys on schemas wide enough to use the hash map.
#[test]
fn json_decode_schema_order_fast_path_falls_back_for_reordered_fields() {
    let schema: SchemaRef = Arc::new(Schema::new(
        (0..8)
            .map(|i| Field::new(format!("f{i}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let out =
        JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&bodies(vec![Some(
            br#"{"f7": 70, "unknown": 99, "f0": 0, "f3": 30, "f1": 10, "f6": 60}"#,
        )]));
    for (index, expected) in [
        Some(0),
        Some(10),
        None,
        Some(30),
        None,
        None,
        Some(60),
        Some(70),
    ]
    .into_iter()
    .enumerate()
    {
        let column = out
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        match expected {
            Some(value) => assert_eq!(column.value(0), value),
            None => assert!(column.is_null(0)),
        }
    }
}

// simd-json rejects documents Jackson tokenizes fine: out-of-range number literals, raw control
// characters inside strings (Flink enables ALLOW_UNESCAPED_CONTROL_CHARS), and content trailing
// the root document (which Flink's pull parser never reads). Those messages retry through the
// Jackson-faithful token walk instead of failing the job / dropping the message.
#[test]
fn json_decode_retries_documents_only_jackson_tokenizes() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let batch = bodies(vec![
        // score is DOUBLE: a beyond-u64 int literal converts via parseDouble of the raw text,
        // and 1e999 overflows to Infinity — per-field successes in Flink, not job failures.
        Some(br#"{"score": 18446744073709551616}"#),
        Some(br#"{"score": 1e999}"#),
        Some(br#"{"id": 1}{"id": 2}"#), // Flink reads ONE document and ignores the rest
        Some(br#"{"id": 3} trailing garbage is never tokenized"#),
        Some(b"{\"name\": \"a\tb\"}"), // a raw TAB inside the string
        Some(br#"[{"score": 1e999}, {"id": 7}]"#), // the retry fans a top-level array out too
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 7);
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.8446744073709552e19);
    assert_eq!(scores.value(1), f64::INFINITY);
    let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((ids.value(2), ids.value(3), ids.value(6)), (1, 3, 7));
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(4), "a\tb");
    assert_eq!(scores.value(5), f64::INFINITY);

    // id is BIGINT: a beyond-long literal fails Long.parseLong PER FIELD — the job dies in strict
    // mode with the row-converter error, and skip mode nulls just the field, keeping the row.
    let bad_long = bodies(vec![Some(
        br#"{"id": 18446744073709551616, "name": "keep"}"#,
    )]);
    let strict = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&bad_long))).is_err());
    let out = JsonDecoder::new(
        json_schema(),
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    )
    .decode(&bad_long);
    assert_eq!(out.num_rows(), 1);
    assert!(out.column(0).is_null(0));
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "keep");
}

// A container token under a scalar (non-STRING) column makes Flink's pull-parser cursor drift:
// the converter coerces getText ("{") without consuming the container, and the row walk's next
// step lands INSIDE it. Under (b BOOLEAN, i INT), {"b":{},"i":7} converts b=false
// (parseBoolean("{")), then mistakes the inner END_OBJECT for the row's end — Flink SUCCEEDS with
// (false, null), so a native panic here would kill a job Flink runs. The message re-decodes
// through the token walk, which replays the drift.
#[test]
fn json_decode_replicates_flinks_cursor_drift() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let lenient = crate::json::JsonEnv {
        lenient: true,
        ..Default::default()
    };
    let bool_int: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i", DataType::Int64, true),
    ]));
    for body in [
        br#"{"b": {}, "i": 7}"#.as_slice(),
        br#"{"b": {"x": 1}, "i": 7}"#,
    ] {
        let batch = bodies(vec![Some(body)]);
        for env in [crate::json::JsonEnv::default(), lenient] {
            let out = JsonDecoder::new(bool_int.clone(), env).decode(&batch);
            assert_eq!(out.num_rows(), 1);
            let b = out
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert!(!b.value(0)); // parseBoolean("{")
            assert!(out.column(1).is_null(0)); // `i` was never matched — the drift ate it
        }
    }

    // The reverse order: parseInt("{") fails the field, so strict mode dies (like Flink); in skip
    // mode the drift again ends the row early — (null, null), not (null, true).
    let int_bool: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("b", DataType::Boolean, true),
    ]));
    let batch = bodies(vec![Some(br#"{"i": {}, "b": true}"#.as_slice())]);
    let strict = JsonDecoder::new(int_bool.clone(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err());
    let out = JsonDecoder::new(int_bool, lenient).decode(&batch);
    assert_eq!(out.num_rows(), 1);
    assert!(out.column(0).is_null(0));
    assert!(out.column(1).is_null(0));

    // Inside a MAP value the drifted walk exits the map at the inner END_OBJECT and the row walk
    // then skips the tail keys — one entry {k: null}, never k2.
    let map: SchemaRef = Arc::new(Schema::new(vec![Field::new_map(
        "m",
        "entries",
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Int64, true),
        false,
        true,
    )]));
    let batch = bodies(vec![Some(br#"{"m": {"k": {}, "k2": 5}}"#.as_slice())]);
    let out = JsonDecoder::new(map, lenient).decode(&batch);
    use arrow::array::MapArray;
    let maps = out.column(0).as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(maps.value_length(0), 1);
    let keys = maps
        .entries()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(keys.value(0), "k");
    assert!(maps.entries().column(1).is_null(0));

    // A nested-array ELEMENT drifts Flink's fan-out loop itself: the walk steps into [2], takes
    // its ']' for the fan-out's END_ARRAY, and stops — one row, the trailing element never read.
    let batch = bodies(vec![Some(br#"[{"id": 1}, [2], {"id": 3}]"#.as_slice())]);
    let out = JsonDecoder::new(json_schema(), lenient).decode(&batch);
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 0), vec![1]);
}

// A FLOAT column parses its literal at float width, like Jackson's getFloatValue (2.14+): the
// tape's f64-then-narrow double rounding is off by one ULP exactly when the f64 lands on an f32
// rounding midpoint — those messages re-decode through the token walk with the raw literal.
#[test]
fn json_decode_float_columns_parse_the_literal_at_float_width() {
    use arrow::array::Float32Array;
    let direct: f32 = "7.038531e-26".parse().unwrap();
    let double_rounded = "7.038531e-26".parse::<f64>().unwrap() as f32;
    assert_ne!(
        direct, double_rounded,
        "the probe literal must be a midpoint case"
    );

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("fl", DataType::Float32, true),
        Field::new("id", DataType::Int64, true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"fl": 7.038531e-26, "id": 1}"#),
        Some(br#"{"fl": 1.5, "id": 2}"#), // midpoint-free literals stay on the fast path
        Some(br#"[{"fl": 7.038531e-26}, {"fl": -7.038531e-26}]"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let floats = out
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(floats.value(0), direct);
    assert_eq!(floats.value(1), 1.5);
    assert_eq!((floats.value(2), floats.value(3)), (direct, -direct));
    assert_eq!(values(&out, 1)[..2], [1, 2]);
}

// An empty input batch flushes to an empty batch of the target schema, not a panic.
#[test]
fn json_decode_empty_batch_yields_empty() {
    let out =
        JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&bodies(vec![]));
    assert_eq!(out.num_rows(), 0);
    assert_eq!(out.schema(), json_schema());
}

// Every scalar type the boundary admits decodes: numbers for the numeric widths (a float for an
// integer column truncates), true/false for BOOLEAN, and strings for DATE and for TIMESTAMP in
// both the SQL and ISO-8601 forms (a bare number is a raw nanosecond epoch).
#[test]
fn json_decode_covers_boundary_scalar_types() {
    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array, Int8Array,
        TimestampNanosecondArray,
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
            br#"{"i8": 1, "i16": -2, "i32": 3.9, "i64": "42", "f32": 2, "f64": "Infinity",
                    "flag": "TRUE", "day": "1970-01-02", "ts": "2026-07-01 12:00:00.123Z"}"#,
        ),
        Some(br#"{"flag": 1}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 3);
    let i8s = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!((i8s.value(0), i8s.value(1)), (-3, 1));
    let i16s = out.column(1).as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!((i16s.value(0), i16s.value(1)), (300, -2));
    let i32s = out.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
    // A float token truncates toward zero under INT/BIGINT (convertToInt); TINYINT/SMALLINT
    // reject float tokens outright (convertToByte falls through to parseByte) — parity-pinned.
    assert_eq!((i32s.value(0), i32s.value(1)), (70000, 3));
    assert_eq!(values(&out, 3), vec![5000000000, 42, 0]);
    assert!(out.column(3).is_null(2));
    let f32s = out
        .column(4)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!((f32s.value(0), f32s.value(1)), (1.5, 2.0));
    let f64s = out
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(f64s.value(0), 2.5);
    assert_eq!(f64s.value(1), f64::INFINITY); // Java's Double.parseDouble spelling
    let flags = out
        .column(6)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(flags.value(0) && flags.value(1)); // parseBoolean is case-insensitive
    assert!(!flags.value(2)); // ... and a number is simply false, never an error
    let days = out
        .column(7)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(days.value(1), 1);
    let ts = out
        .column(8)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let expected = 1_782_907_200_123_000_000i64; // 2026-07-01T12:00:00.123Z
                                                 // The trailing 'Z' is the tolerated LTZ shape (divergences/21) — same instant.
    assert_eq!((ts.value(0), ts.value(1)), (expected, expected));
    assert!(ts.is_null(2));
}

// TIME parses SQL_TIME_FORMAT and stores whole seconds at the column's Arrow unit (Flink's
// toSecondOfDay() * 1000 discards the fraction whatever the declared precision); VARBINARY is
// Jackson's base64 read, declared length not enforced. Both hold on the simd path and — riding as
// text — on the decimal-bearing arrow-json path.
#[test]
fn json_decode_time_truncates_and_binary_follows_jackson() {
    use arrow::array::{Time32MillisecondArray, Time32SecondArray, Time64NanosecondArray};
    use arrow::datatypes::TimeUnit;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("t0", DataType::Time32(TimeUnit::Second), true),
        Field::new("t3", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("t9", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("b", DataType::Binary, true),
    ]));
    let batch = bodies(vec![Some(
        br#"{"t0": "12:34:56.789", "t3": "12:34:56.789", "t9": "12:34:56.123456789",
                "b": "AQIDBAUGBwg="}"#,
    )]);
    let out = JsonDecoder::new(schema.clone(), crate::json::JsonEnv::default()).decode(&batch);
    let secs = 12 * 3600 + 34 * 60 + 56;
    let t0 = out
        .column(0)
        .as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap();
    assert_eq!(t0.value(0), secs as i32);
    let t3 = out
        .column(1)
        .as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap();
    assert_eq!(t3.value(0), (secs * 1000) as i32); // the .789 is gone
    let t9 = out
        .column(2)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap();
    assert_eq!(t9.value(0), secs as i64 * 1_000_000_000);
    let b = out
        .column(3)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(b.value(0), [1, 2, 3, 4, 5, 6, 7, 8]);
    // Missing base64 padding fails like Jackson's MIME read; under skip mode the field nulls.
    let bad = bodies(vec![Some(br#"{"b": "AQ"}"#)]);
    let strict = schema.clone();
    assert!(catch_unwind(AssertUnwindSafe(|| JsonDecoder::new(
        strict,
        crate::json::JsonEnv::default()
    )
    .decode(&bad)))
    .is_err());
    let lenient = JsonDecoder::new(
        schema,
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    )
    .decode(&bad);
    assert!(lenient.column(3).is_null(0));

    // The decimal-bearing schema keeps the same envelope through the text-restored path.
    let mixed: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("dec", DataType::Decimal128(5, 2), true),
        Field::new("t3", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("b", DataType::Binary, true),
    ]));
    let batch = bodies(vec![Some(
        br#"{"dec": 1.235, "t3": "12:34:56.789", "b": "AQID"}"#,
    )]);
    let out = JsonDecoder::new(mixed, crate::json::JsonEnv::default()).decode(&batch);
    let dec = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(dec.value(0), 124); // HALF_UP
    let t3 = out
        .column(1)
        .as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap();
    assert_eq!(t3.value(0), (secs * 1000) as i32);
    let b = out
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(b.value(0), [1, 2, 3]);
}

// A DECIMAL-bearing schema rides arrow-json, whose map decoder keeps every entry of a
// duplicate-keyed object; Flink builds a java.util.Map — one entry per key, last value. The
// decode collapses to last-value-first-position, like the simd path.
#[test]
fn json_decode_decimal_path_collapses_duplicate_map_keys() {
    use arrow::array::MapArray;
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new_map(
            "m",
            "entries",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Decimal128(5, 2), true),
            false,
            true,
        ),
        Field::new("dec", DataType::Decimal128(5, 2), true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"m": {"k": 1.234, "j": 2, "k": 3.456}, "dec": 1.5}"#),
        Some(br#"{"m": {"u": 7}}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let maps = out.column(0).as_any().downcast_ref::<MapArray>().unwrap();
    let keys = maps
        .entries()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = maps
        .entries()
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    // Row 0: k keeps its first position with the LAST value (HALF_UP of 3.456), j follows.
    assert_eq!(maps.value_length(0), 2);
    assert_eq!((keys.value(0), values.value(0)), ("k", 346));
    assert_eq!((keys.value(1), values.value(1)), ("j", 200));
    // Row 1 (no duplicates) is untouched.
    assert_eq!(maps.value_length(1), 1);
    assert_eq!((keys.value(2), values.value(2)), ("u", 700));
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
    let iso = crate::json::JsonEnv {
        mode: flink_text::TimestampMode::Iso8601,
        ..Default::default()
    };
    let decode = |schema: &SchemaRef, env, body: &'static [u8]| {
        let schema = schema.clone();
        let batch = bodies(vec![Some(body)]);
        catch_unwind(AssertUnwindSafe(move || {
            JsonDecoder::new(schema, env).decode(&batch)
        }))
    };
    // SQL mode: space separator only; ISO mode: 'T' only (seconds optional there).
    assert!(decode(&ts_schema, sql, br#"{"ts": "2026-07-01T12:00:00"}"#).is_err());
    assert!(decode(&ts_schema, iso, br#"{"ts": "2026-07-01 12:00:00"}"#).is_err());
    assert!(decode(&ts_schema, iso, br#"{"ts": "2026-07-01T12:00"}"#).is_ok());
    // A bare number is not a Flink timestamp or date.
    assert!(decode(&ts_schema, sql, br#"{"ts": 123456789}"#).is_err());
    let day_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("day", DataType::Date32, true)]));
    assert!(decode(&day_schema, sql, br#"{"day": 42}"#).is_err());
    assert!(decode(&day_schema, sql, br#"{"day": "2026-7-1"}"#).is_err());
    // STRING coercions: ints/bools/containers echo exactly, a float literal fails loudly.
    let str_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let echoed = decode(
        &str_schema,
        sql,
        b"{\"s\": {\"a\": 1, \"b\": [true, null, \"x\\n\"], \"a\": 2}}",
    )
    .unwrap();
    let strings = echoed
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Duplicate keys collapse last-value-first-position, like Jackson's tree; the escaped
    // newline round-trips through Jackson-style escaping.
    assert_eq!(strings.value(0), "{\"a\":2,\"b\":[true,null,\"x\\n\"]}");
    let echoed = decode(&str_schema, sql, br#"{"s": 42}"#).unwrap();
    let strings = echoed
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
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

    let row = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
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
    let map_values = tags
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
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
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(d.value(0), 124); // HALF_UP, not truncation
    assert!(d.is_null(1)); // precision overflow → NULL (DecimalData.fromBigDecimal), not an error
    let wide = out
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(wide.value(0), 12345678901234567890i128); // exact raw literal, HALF_UP at scale 20
    let row = out
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let p = row
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(p.value(0), -124); // nested decimals get the same conversion; strings trim+parse
    assert!(row.is_null(1));
}

// Unknown keys are skipped and a duplicated field keeps its last value — Jackson (hence Flink)
// and arrow-json agree on both.
#[test]
fn json_decode_skips_unknown_keys_and_keeps_last_duplicate() {
    let batch = bodies(vec![Some(
        br#"{"extra": [1, {"x": 2}], "id": 1, "name": "a", "id": 5}"#,
    )]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(values(&out, 0), vec![5]);
}

// DECIMAL columns route to the raw-literal (arrow-json) path: a number with more significant
// digits than an f64 carries still decodes exactly, in number and string position alike.
#[test]
fn json_decode_decimal_stays_exact_beyond_f64_precision() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(30, 10),
        true,
    )]));
    let batch = bodies(vec![
        Some(br#"{"d": 12345678901234567.8901234567}"#),
        Some(br#"{"d": "12345678901234567.8901234567"}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
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
    let batch = bodies(vec![Some(br#"42"#)]);
    JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
}

// Flink's `json` format fans a top-level array out into one row per element (`processArray`);
// an empty array contributes no row, and surrounding whitespace is insignificant.
#[test]
fn json_decode_fans_out_top_level_arrays() {
    let batch = bodies(vec![
        Some(br#"  [ {"id": 1, "name": "a"} , {"id": 2} ]  "#.as_slice()),
        Some(br#"[]"#),
        Some(br#"{"id": 3}"#),
        Some(br#"[{"id": 4, "score": 4.5}]"#),
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 4);
    assert_eq!(values(&out, 0), vec![1, 2, 3, 4]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "a");
    assert!(names.is_null(1));
}

// A non-object array element fails the whole message in strict mode (any element failure fails
// Flink's deserialize) and drops alone under ignore-parse-errors, keeping its good siblings. A bad
// *value* inside an element stays the usual per-field null.
#[test]
fn json_decode_array_element_granularity_follows_flink() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let strict = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default());
    for body in [
        br#"[{"id": 1}, 5, {"id": 3}]"#.as_slice(),
        br#"[{"id": 1}, null]"#,
        br#"[[{"id": 1}]]"#,
    ] {
        let batch = bodies(vec![Some(body)]);
        assert!(
            catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err(),
            "strict decode must fail: {}",
            String::from_utf8_lossy(body)
        );
    }
    let lenient = JsonDecoder::new(
        json_schema(),
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    );
    let out = lenient.decode(&bodies(vec![Some(br#"[{"id": 1}, 5, null, {"id": 3}]"#)]));
    assert_eq!(values(&out, 0), vec![1, 3]);
    // A nested-array element drifts Flink's element loop itself (replayed via the token walk):
    // only the prefix before it survives, and the tail is never read.
    let out = lenient.decode(&bodies(vec![Some(
        br#"[{"id": 1}, 5, null, [7], {"id": 3}]"#,
    )]));
    assert_eq!(values(&out, 0), vec![1]);
    // A malformed array-rooted document still drops whole, never element by element.
    let out = lenient.decode(&bodies(vec![Some(br#"[{"id": 1}, {"id": }]"#)]));
    assert_eq!(out.num_rows(), 0);
    // A bad value inside an element nulls the field and keeps the element's row.
    let out = lenient.decode(&bodies(vec![Some(
        br#"[{"id": 1}, {"id": "junk"}, {"id": 3}]"#,
    )]));
    assert_eq!(out.num_rows(), 3);
    let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((ids.value(0), ids.value(2)), (1, 3));
    assert!(ids.is_null(1));
}

// The decimal-bearing (arrow-json) subpath fans arrays out with the same granularity as the simd
// path, keeping each element's exact raw literal for the decimal parse.
#[test]
fn json_decimal_path_fans_out_top_level_arrays() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(30, 10),
        true,
    )]));
    let strict = JsonDecoder::new(schema.clone(), crate::json::JsonEnv::default());
    let out = strict.decode(&bodies(vec![
        Some(br#" [ {"d": 12345678901234567.8901234567}, {"d": 1.5} ] "#.as_slice()),
        Some(br#"[]"#),
        Some(br#"{"d": 2.5}"#),
    ]));
    assert_eq!(out.num_rows(), 3);
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    // The raw literal survives f64-impossible precision through the element split.
    assert_eq!(d.value(0), 123456789012345678901234567i128);
    assert_eq!(d.value(1), 15_000_000_000i128);
    for body in [
        br#"[{"d": 1.5}, 7]"#.as_slice(),
        br#"[{"d": 1.5}, null]"#,
        br#"[[{"d": 1}]]"#,
    ] {
        let batch = bodies(vec![Some(body)]);
        assert!(
            catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err(),
            "strict decode must fail: {}",
            String::from_utf8_lossy(body)
        );
    }
    let lenient = JsonDecoder::new(
        schema,
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    );
    let out = lenient.decode(&bodies(vec![Some(
        br#"[{"d": 1.5}, 7, null, [7], {"d": "junk"}, {"d": 2.5}]"#,
    )]));
    // Non-object elements drop alone; the bad decimal *value* nulls per field and keeps its row.
    assert_eq!(out.num_rows(), 3);
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        (d.value(0), d.value(2)),
        (15_000_000_000i128, 25_000_000_000i128)
    );
    assert!(d.is_null(1));
    let out = lenient.decode(&bodies(vec![Some(br#"[{"d": 1.5}, {"d": }]"#)]));
    assert_eq!(out.num_rows(), 0);
}

// A CDC envelope never fans out a top-level array. Debezium/OGG decode through Flink's
// deprecated one-row entry, which unwraps an array holding exactly one envelope — and in skip
// mode junk elements are dropped from the fan-out first, so a lone envelope among junk still
// decodes there. Every other shape is corrupt, and Maxwell/Canal (tree-converter entry) reject
// any array root — on the simd path and, with a decimal-bearing physical schema, on the
// arrow-json path.
#[test]
fn cdc_array_roots_unwrap_or_reject_like_flink() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let env1 = r#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c"}"#;
    let env2 = r#"{"before":null,"after":{"id":2,"name":"b","score":2.5},"op":"c"}"#;
    let decimal_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("score", DataType::Decimal128(5, 2), true),
    ]));
    let denv1 = r#"{"before":null,"after":{"id":1,"score":1.5},"op":"c"}"#;
    let denv2 = r#"{"before":null,"after":{"id":2,"score":2.5},"op":"c"}"#;
    for (schema, env1, env2) in [(json_schema(), env1, env2), (decimal_schema, denv1, denv2)] {
        let strict = new_decoder(FORMAT_DEBEZIUM_JSON, schema.clone(), "", "", 0, false, "");
        let skipping = new_decoder(FORMAT_DEBEZIUM_JSON, schema, "", "", 0, true, "");
        let rows = |decoder: &MessageDecoder, body: &str| {
            decoder
                .decode(&bodies(vec![Some(body.as_bytes())]))
                .num_rows()
        };
        let panics = |decoder: &MessageDecoder, body: &str| {
            let batch = bodies(vec![Some(body.as_bytes())]);
            catch_unwind(AssertUnwindSafe(|| decoder.decode(&batch))).is_err()
        };
        let single = format!("[{env1}]");
        assert_eq!(rows(&strict, &single), 1);
        assert_eq!(rows(&skipping, &single), 1);
        let pair = format!("[{env1},{env2}]");
        assert!(panics(&strict, &pair));
        assert_eq!(rows(&skipping, &pair), 0);
        let with_junk = format!("[{env1},1]");
        assert!(panics(&strict, &with_junk));
        assert_eq!(rows(&skipping, &with_junk), 1);
        assert!(panics(&strict, "[]"));
        assert_eq!(rows(&skipping, "[]"), 0);
    }
    let maxwell = r#"[{"data":{"id":1,"name":"a","score":1.5},"type":"insert"}]"#;
    let batch = bodies(vec![Some(maxwell.as_bytes())]);
    let strict = new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "");
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err());
    let skipping = new_decoder(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, true, "");
    assert_eq!(skipping.decode(&batch).num_rows(), 0);
}

// A decode failure crossing the C ABI keeps its cause: the nonzero return is followed by the
// version-2 error channel serving the format's real panic text, which the connector folds into
// the failure it raises with the bucket's topic/partition/offsets.
#[test]
fn c_abi_decode_failure_serves_the_panic_text() {
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    extern "C" fn sentinel(_: i64, _: i64, _: i64, _: i64, _: i64) -> i32 {
        99
    }
    extern "C" fn error_sentinel(_: i64, _: *mut i32) -> *const u8 {
        std::ptr::null()
    }
    let mut driver = FormatDriver {
        decode_body_batch: sentinel,
        decode_last_error: error_sentinel,
    };
    assert_eq!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_2, &mut driver),
        0
    );

    let decoder = new_decoder(FORMAT_JSON, json_schema(), "", "", 0, false, "");
    let body = bodies(vec![Some(b"{\"id\": not json")]);
    let mut in_array = FFI_ArrowArray::empty();
    let mut in_schema = FFI_ArrowSchema::empty();
    export_record_batch(
        body,
        &mut in_array as *mut FFI_ArrowArray as i64,
        &mut in_schema as *mut FFI_ArrowSchema as i64,
    );
    let mut out_array = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();
    let rc = silence_expected_decode_panics(|| {
        (driver.decode_body_batch)(
            &decoder as *const MessageDecoder as i64,
            &mut in_array as *mut FFI_ArrowArray as i64,
            &mut in_schema as *mut FFI_ArrowSchema as i64,
            &mut out_array as *mut FFI_ArrowArray as i64,
            &mut out_schema as *mut FFI_ArrowSchema as i64,
        )
    });
    assert_ne!(rc, 0);

    let mut len = 0i32;
    let pointer = (driver.decode_last_error)(&decoder as *const MessageDecoder as i64, &mut len);
    assert!(!pointer.is_null());
    let message =
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(pointer, len as usize) }).unwrap();
    assert!(
        message.contains("failed to decode JSON record"),
        "error channel must carry the decode's own panic text, got: {message}"
    );
}
