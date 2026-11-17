use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_json::bench::{FormatDecode, JsonDecode};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn json(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("format/json/decode");
    for rows in [16, 1024, 16384] {
        for width in [8, 264, 4096] {
            for projected in [false, true] {
                let document =
                    format!(r#"{{"id":42,"name":"{}","score":1.25}}"#, "x".repeat(width));
                let bodies = RecordBatch::try_from_iter(vec![(
                    "body",
                    Arc::new(BinaryArray::from_iter(std::iter::repeat_n(
                        Some(document.as_bytes()),
                        rows + 1,
                    ))) as ArrayRef,
                )])
                .unwrap()
                .slice(1, rows);
                let mut fields = vec![Field::new("id", DataType::Int64, true)];
                if !projected {
                    fields.extend([
                        Field::new("name", DataType::Utf8, true),
                        Field::new("score", DataType::Float64, true),
                    ]);
                }
                let decoder = JsonDecode::new(Arc::new(Schema::new(fields)));
                let label = format!("{rows}/bytes={width}/projected={projected}");
                decoder.decode(&bodies);
                let (output, allocations) = measure(|| decoder.decode(&bodies));
                assert_eq!(output.num_rows(), rows);
                report(&label, bodies.columns(), output.columns(), allocations);
                group.throughput(Throughput::Elements(rows as u64));
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(decoder.decode(std::hint::black_box(&bodies))))
                });
            }
        }
    }
    group.finish();
}
fn bodies_of(documents: &[String], rows: usize) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "body",
        Arc::new(BinaryArray::from_iter(
            (0..rows).map(|row| Some(documents[row % documents.len()].as_bytes())),
        )) as ArrayRef,
    )])
    .unwrap()
}

fn bench_decode(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    label: &str,
    decoder: &FormatDecode,
    bodies: &RecordBatch,
    rows: usize,
) {
    decoder.decode(bodies);
    let (output, allocations) = measure(|| decoder.decode(bodies));
    assert_eq!(output.num_rows(), rows);
    report(label, bodies.columns(), output.columns(), allocations);
    group.throughput(Throughput::Elements(bodies.num_rows() as u64));
    group.bench_function(BenchmarkId::from_parameter(label), |b| {
        b.iter(|| std::hint::black_box(decoder.decode(std::hint::black_box(bodies))))
    });
}

/// Canal messages as Canal writes them (envelope fields included, string values), inserts alone
/// and updates with a partial `old`: the old-image presence scan only matters for the updates.
fn canal(c: &mut Criterion) {
    let mut group = c.benchmark_group("format/canal-json/decode");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("tenant", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let rows = 8192;
    let message = |id: usize, kind: &str, old: &str| {
        format!(
            r#"{{"data":[{{"id":"{id}","tenant":"t{}","name":"n{id}","amount":"{}"}}],"database":"shop","es":1589373560000,"id":{id},"isDdl":false,"mysqlType":{{"id":"bigint(20)","tenant":"varchar(16)","name":"varchar(64)","amount":"bigint(20)"}},"old":{old},"pkNames":["id"],"sql":"","sqlType":{{"id":-5,"tenant":12,"name":12,"amount":-5}},"table":"orders","ts":1589373560798,"type":"{kind}"}}"#,
            id % 64,
            id % 1000
        )
    };
    let inserts: Vec<String> = (0..rows).map(|id| message(id, "INSERT", "null")).collect();
    let updates: Vec<String> = (0..rows)
        .map(|id| message(id, "UPDATE", r#"[{"amount":"7"}]"#))
        .collect();
    let decoder = FormatDecode::new(9, schema);
    bench_decode(
        &mut group,
        "inserts",
        &decoder,
        &bodies_of(&inserts, rows),
        rows,
    );
    bench_decode(
        &mut group,
        "updates",
        &decoder,
        &bodies_of(&updates, rows),
        2 * rows,
    );
    group.finish();
}

/// A MAP<STRING, BIGINT> column, whose entries follow Flink's HashMap iteration order.
fn json_map(c: &mut Criterion) {
    let mut group = c.benchmark_group("format/json/decode-map");
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]
            .into(),
        ),
        false,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("attrs", DataType::Map(entries, false), true),
    ]));
    let rows = 8192;
    for keys in [4, 32] {
        let documents: Vec<String> = (0..rows)
            .map(|id| {
                let attrs: Vec<String> = (0..keys)
                    .map(|key| format!(r#""attribute_{key}":{}"#, id + key))
                    .collect();
                format!(r#"{{"id":{id},"attrs":{{{}}}}}"#, attrs.join(","))
            })
            .collect();
        let decoder = FormatDecode::new(0, schema.clone());
        let label = format!("{rows}/keys={keys}");
        bench_decode(
            &mut group,
            &label,
            &decoder,
            &bodies_of(&documents, rows),
            rows,
        );
    }
    group.finish();
}

criterion_group!(benches, json, canal, json_map);
criterion_main!(benches);
