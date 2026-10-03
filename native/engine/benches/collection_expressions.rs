use arrow::array::{
    ArrayRef, BooleanBuilder, Int32Builder, ListBuilder, RecordBatch, StringBuilder,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn plan() -> CalcProgram {
    CalcProgram::new(vec![6, 0], vec![165, 0], vec![1, 0], vec![], vec![], vec![])
}

fn fixture(
    kind: &str,
    rows: usize,
    width: usize,
    domain: usize,
    bytes: usize,
    nullable: bool,
) -> (ArrayRef, ArrayRef) {
    let mut integers = ListBuilder::new(Int32Builder::new());
    let mut expected_integers = ListBuilder::new(Int32Builder::new());
    let mut booleans = ListBuilder::new(BooleanBuilder::new());
    let mut expected_booleans = ListBuilder::new(BooleanBuilder::new());
    let mut strings = ListBuilder::new(StringBuilder::new());
    let mut expected_strings = ListBuilder::new(StringBuilder::new());
    let text: Vec<_> = (0..domain)
        .map(|i| format!("{i:04}-é中😀{}", "x".repeat(bytes)))
        .collect();
    for row in 0..rows + 1 {
        let valid = !nullable || row % 7 != 0;
        if valid {
            let mut seen = std::collections::BTreeSet::new();
            for col in 0..width {
                let value = if nullable && col % 9 == 0 {
                    None
                } else {
                    Some((row + col) % domain)
                };
                let first = seen.insert(value);
                match kind {
                    "integer" => {
                        integers.values().append_option(value.map(|v| v as i32));
                        if first {
                            expected_integers
                                .values()
                                .append_option(value.map(|v| v as i32));
                        }
                    }
                    "boolean" => {
                        booleans.values().append_option(value.map(|v| v != 0));
                        if first {
                            expected_booleans
                                .values()
                                .append_option(value.map(|v| v != 0));
                        }
                    }
                    _ => {
                        strings
                            .values()
                            .append_option(value.map(|v| text[v].as_str()));
                        if first {
                            expected_strings
                                .values()
                                .append_option(value.map(|v| text[v].as_str()));
                        }
                    }
                }
            }
        }
        match kind {
            "integer" => {
                integers.append(valid);
                expected_integers.append(valid);
            }
            "boolean" => {
                booleans.append(valid);
                expected_booleans.append(valid);
            }
            _ => {
                strings.append(valid);
                expected_strings.append(valid);
            }
        }
    }
    match kind {
        "integer" => (
            Arc::new(integers.finish().slice(1, rows)),
            Arc::new(expected_integers.finish().slice(1, rows)),
        ),
        "boolean" => (
            Arc::new(booleans.finish().slice(1, rows)),
            Arc::new(expected_booleans.finish().slice(1, rows)),
        ),
        _ => (
            Arc::new(strings.finish().slice(1, rows)),
            Arc::new(expected_strings.finish().slice(1, rows)),
        ),
    }
}

fn run(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("collection_expressions");
    for rows in [16, 1024, 16384] {
        for width in [8, 64] {
            for nullable in [false, true] {
                for kind in ["integer", "boolean", "string"] {
                    let domains = if kind == "boolean" {
                        vec![2]
                    } else {
                        vec![4, width]
                    };
                    let payloads = if kind == "string" {
                        vec![8, 264]
                    } else {
                        vec![0]
                    };
                    for domain in domains {
                        for &bytes in &payloads {
                            let (input, expected) =
                                fixture(kind, rows, width, domain, bytes, nullable);
                            let batch = RecordBatch::try_from_iter([("value", input)]).unwrap();
                            let mut calc = plan();
                            let warm = calc.run(batch.clone());
                            assert_eq!(warm.column(0).as_ref(), expected.as_ref());
                            let label = format!("{kind}/{rows}/width={width}/domain={domain}/bytes={bytes}/nulls={nullable}");
                            let (out, allocations) = measure(|| calc.run(batch.clone()));
                            report(&label, batch.columns(), out.columns(), allocations);
                            group.throughput(Throughput::Elements(rows as u64));
                            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                                b.iter(|| {
                                    std::hint::black_box(
                                        calc.run(std::hint::black_box(batch.clone())),
                                    )
                                })
                            });
                        }
                    }
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, run);
criterion_main!(benches);
