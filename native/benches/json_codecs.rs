//! End-to-end Nexmark JSON codec comparison. Every decode case starts with the same Kafka message
//! bytes and ends with the same Arrow schema; every encode case starts with the same Arrow batch
//! and ends with one JSON document per row. Run with:
//! `cargo bench --release --features json --bench json_codecs`.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::json::writer::LineDelimited;
use arrow::json::{ReaderBuilder, WriterBuilder};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde::{Deserialize, Serialize};
use streamfusion::bench::JsonDecode;

const ROWS: usize = 8192;
const BLOCK: usize = 50;
const STATES: [&str; 6] = ["OR", "ID", "CA", "WA", "NY", "TX"];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Event {
    #[serde(rename = "event_type")]
    event_type: i32,
    person: Option<Person>,
    auction: Option<Auction>,
    bid: Option<Bid>,
    #[serde(rename = "dateTime")]
    date_time: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Person {
    id: i64,
    name: String,
    email_address: String,
    credit_card: String,
    city: String,
    state: String,
    #[serde(rename = "dateTime")]
    date_time: i64,
    extra: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Auction {
    id: i64,
    item_name: String,
    description: String,
    initial_bid: i64,
    reserve: i64,
    #[serde(rename = "dateTime")]
    date_time: i64,
    expires: i64,
    seller: i64,
    category: i64,
    extra: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Bid {
    auction: i64,
    bidder: i64,
    price: i64,
    channel: String,
    url: String,
    #[serde(rename = "dateTime")]
    date_time: i64,
    extra: String,
}

fn struct_field(name: &str, fields: Vec<Field>) -> Field {
    Field::new(name, DataType::Struct(Fields::from(fields)), true)
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_type", DataType::Int32, false),
        struct_field(
            "person",
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("emailAddress", DataType::Utf8, false),
                Field::new("creditCard", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("dateTime", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ],
        ),
        struct_field(
            "auction",
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("itemName", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initialBid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("dateTime", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ],
        ),
        struct_field(
            "bid",
            vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("dateTime", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ],
        ),
        Field::new("dateTime", DataType::Int64, false),
    ]))
}

fn document(i: usize) -> Vec<u8> {
    let block = i / BLOCK;
    let pos = i % BLOCK;
    let ts = (block * 1000 + pos * 10) as i64;
    let json = if pos == 0 {
        format!(
            r#"{{"event_type":0,"person":{{"id":{block},"name":"p-{block}","emailAddress":"e-{block}","creditCard":"1234","city":"c-{}","state":"{}","dateTime":{ts},"extra":"x"}},"auction":null,"bid":null,"dateTime":{ts}}}"#,
            block % 1000,
            STATES[block % STATES.len()]
        )
    } else if pos <= 3 {
        let id = block * 3 + pos - 1;
        format!(
            r#"{{"event_type":1,"person":null,"auction":{{"id":{id},"itemName":"i-{id}","description":"d-{id}","initialBid":10,"reserve":50,"dateTime":{ts},"expires":{},"seller":{block},"category":{},"extra":"x"}},"bid":null,"dateTime":{ts}}}"#,
            ts + 20_000,
            block % 100
        )
    } else {
        let auction = block * 3 + pos % 3;
        format!(
            r#"{{"event_type":2,"person":null,"auction":null,"bid":{{"auction":{auction},"bidder":{pos},"price":{},"channel":"ch-{}","url":"https://n.test/{auction}","dateTime":{ts},"extra":"x"}},"dateTime":{ts}}}"#,
            i % 1000 + 1,
            pos % 8
        )
    };
    json.into_bytes()
}

fn corpus() -> Vec<Vec<u8>> {
    (0..ROWS).map(document).collect()
}

fn ndjson(documents: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(documents.iter().map(Vec::len).sum::<usize>() + ROWS);
    for document in documents {
        out.extend_from_slice(document);
        out.push(b'\n');
    }
    out
}

fn arrow_json_decode(schema: &SchemaRef, input: &[u8]) -> RecordBatch {
    let mut reader = ReaderBuilder::new(schema.clone())
        .with_batch_size(ROWS)
        .build(Cursor::new(input))
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    batch
}

fn arrow_json_decode_documents(schema: &SchemaRef, documents: &[Vec<u8>]) -> RecordBatch {
    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_batch_size(ROWS)
        .build_decoder()
        .unwrap();
    for document in documents {
        assert_eq!(decoder.decode(document).unwrap(), document.len());
        assert_eq!(decoder.decode(b"\n").unwrap(), 1);
    }
    decoder.flush().unwrap().unwrap()
}

fn typed_to_arrow(schema: &SchemaRef, rows: &[Event]) -> RecordBatch {
    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_batch_size(ROWS)
        .build_decoder()
        .unwrap();
    decoder.serialize(rows).unwrap();
    decoder.flush().unwrap().unwrap()
}

fn sonic_decode(schema: &SchemaRef, documents: &[Vec<u8>]) -> RecordBatch {
    let rows: Vec<Event> = documents
        .iter()
        .map(|document| sonic_rs::from_slice(document).unwrap())
        .collect();
    typed_to_arrow(schema, &rows)
}

fn simd_decode(schema: &SchemaRef, documents: &[Vec<u8>]) -> RecordBatch {
    let rows: Vec<Event> = documents
        .iter()
        .map(|document| {
            // simd-json's in-situ parser requires writable padded input; this copy is part of the
            // real JSON-bytes-to-Arrow cost and is therefore deliberately inside the measurement.
            let mut input = document.clone();
            simd_json::serde::from_slice(&mut input).unwrap()
        })
        .collect();
    typed_to_arrow(schema, &rows)
}

fn production_simd_decode(schema: &SchemaRef, documents: &[Vec<u8>]) -> RecordBatch {
    let refs: Vec<&[u8]> = documents.iter().map(Vec::as_slice).collect();
    let bodies = BinaryArray::from(refs);
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::Binary,
            false,
        )])),
        vec![Arc::new(bodies)],
    )
    .unwrap();
    JsonDecode::new(schema.clone()).decode(&input)
}

fn field<'a, T: Array + 'static>(row: &'a StructArray, name: &str) -> &'a T {
    row.column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}

fn arrow_to_events(batch: &RecordBatch) -> Vec<Event> {
    let event_types = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let people = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let auctions = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let bids = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let date_times = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| Event {
            event_type: event_types.value(i),
            person: (!people.is_null(i)).then(|| Person {
                id: field::<Int64Array>(people, "id").value(i),
                name: field::<StringArray>(people, "name").value(i).to_owned(),
                email_address: field::<StringArray>(people, "emailAddress")
                    .value(i)
                    .to_owned(),
                credit_card: field::<StringArray>(people, "creditCard")
                    .value(i)
                    .to_owned(),
                city: field::<StringArray>(people, "city").value(i).to_owned(),
                state: field::<StringArray>(people, "state").value(i).to_owned(),
                date_time: field::<Int64Array>(people, "dateTime").value(i),
                extra: field::<StringArray>(people, "extra").value(i).to_owned(),
            }),
            auction: (!auctions.is_null(i)).then(|| Auction {
                id: field::<Int64Array>(auctions, "id").value(i),
                item_name: field::<StringArray>(auctions, "itemName")
                    .value(i)
                    .to_owned(),
                description: field::<StringArray>(auctions, "description")
                    .value(i)
                    .to_owned(),
                initial_bid: field::<Int64Array>(auctions, "initialBid").value(i),
                reserve: field::<Int64Array>(auctions, "reserve").value(i),
                date_time: field::<Int64Array>(auctions, "dateTime").value(i),
                expires: field::<Int64Array>(auctions, "expires").value(i),
                seller: field::<Int64Array>(auctions, "seller").value(i),
                category: field::<Int64Array>(auctions, "category").value(i),
                extra: field::<StringArray>(auctions, "extra").value(i).to_owned(),
            }),
            bid: (!bids.is_null(i)).then(|| Bid {
                auction: field::<Int64Array>(bids, "auction").value(i),
                bidder: field::<Int64Array>(bids, "bidder").value(i),
                price: field::<Int64Array>(bids, "price").value(i),
                channel: field::<StringArray>(bids, "channel").value(i).to_owned(),
                url: field::<StringArray>(bids, "url").value(i).to_owned(),
                date_time: field::<Int64Array>(bids, "dateTime").value(i),
                extra: field::<StringArray>(bids, "extra").value(i).to_owned(),
            }),
            date_time: date_times.value(i),
        })
        .collect()
}

#[derive(Debug)]
struct EncodedBatch {
    bytes: Vec<u8>,
    /// Exclusive end of each newline-delimited document in `bytes`.
    row_ends: Vec<usize>,
}

fn row_ends(bytes: &[u8]) -> Vec<usize> {
    bytes
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == b'\n').then_some(index + 1))
        .collect()
}

fn arrow_json_encode(batch: &RecordBatch) -> EncodedBatch {
    let mut output = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, LineDelimited>(&mut output);
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    let row_ends = row_ends(&output);
    EncodedBatch {
        bytes: output,
        row_ends,
    }
}

fn sonic_encode(batch: &RecordBatch) -> EncodedBatch {
    let rows = arrow_to_events(batch);
    let mut bytes = Vec::new();
    let mut row_ends = Vec::with_capacity(rows.len());
    for row in &rows {
        sonic_rs::to_writer(&mut bytes, row).unwrap();
        bytes.push(b'\n');
        row_ends.push(bytes.len());
    }
    EncodedBatch { bytes, row_ends }
}

fn simd_encode(batch: &RecordBatch) -> EncodedBatch {
    let rows = arrow_to_events(batch);
    let mut bytes = Vec::new();
    let mut row_ends = Vec::with_capacity(rows.len());
    for row in &rows {
        simd_json::serde::to_writer(&mut bytes, row).unwrap();
        bytes.push(b'\n');
        row_ends.push(bytes.len());
    }
    EncodedBatch { bytes, row_ends }
}

fn bench_json_codecs(c: &mut Criterion) {
    let schema = schema();
    let documents = corpus();
    let ndjson = ndjson(&documents);
    let expected = arrow_json_decode(&schema, &ndjson);
    assert_eq!(arrow_json_decode_documents(&schema, &documents), expected);
    assert_eq!(sonic_decode(&schema, &documents), expected);
    assert_eq!(simd_decode(&schema, &documents), expected);
    assert_eq!(production_simd_decode(&schema, &documents), expected);

    let mut decode = c.benchmark_group("nexmark_json_to_arrow");
    decode.throughput(Throughput::Bytes(ndjson.len() as u64));
    decode.bench_function("arrow-json", |b| {
        b.iter(|| {
            black_box(arrow_json_decode_documents(
                black_box(&schema),
                black_box(&documents),
            ))
        })
    });
    decode.bench_function("sonic-rs_typed_adapter", |b| {
        b.iter(|| black_box(sonic_decode(black_box(&schema), black_box(&documents))))
    });
    decode.bench_function("simd-json_typed_adapter", |b| {
        b.iter(|| black_box(simd_decode(black_box(&schema), black_box(&documents))))
    });
    decode.bench_function("simd-json_streamfusion_direct", |b| {
        b.iter(|| {
            black_box(production_simd_decode(
                black_box(&schema),
                black_box(&documents),
            ))
        })
    });
    decode.finish();

    // Validate serialization semantically because field order and explicit-null spelling are not
    // meaningful JSON differences. Re-decoding all outputs to the canonical Arrow batch proves
    // that every measured encoder performs the same logical work.
    let arrow_encoded = arrow_json_encode(&expected);
    let sonic_encoded = sonic_encode(&expected);
    let simd_encoded = simd_encode(&expected);
    for encoded in [&arrow_encoded, &sonic_encoded, &simd_encoded] {
        assert_eq!(encoded.row_ends.len(), ROWS);
        assert_eq!(encoded.row_ends.last(), Some(&encoded.bytes.len()));
        assert_eq!(arrow_json_decode(&schema, &encoded.bytes), expected);
    }

    let mut encode = c.benchmark_group("nexmark_arrow_to_json");
    encode.throughput(Throughput::Elements(ROWS as u64));
    encode.bench_function("arrow-json", |b| {
        b.iter(|| black_box(arrow_json_encode(black_box(&expected))))
    });
    encode.bench_function("sonic-rs_typed_adapter", |b| {
        b.iter(|| black_box(sonic_encode(black_box(&expected))))
    });
    encode.bench_function("simd-json_typed_adapter", |b| {
        b.iter(|| black_box(simd_encode(black_box(&expected))))
    });
    encode.finish();
}

criterion_group!(benches, bench_json_codecs);
criterion_main!(benches);
