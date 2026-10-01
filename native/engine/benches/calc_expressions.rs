use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_bridge::timestamp::timestamps_from_millis;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
#[derive(Clone)]
enum Arg {
    Column(i64),
    String(&'static str),
    Integer(i64),
    Double(f64),
}
#[derive(Clone)]
struct Plan {
    name: String,
    kind: i64,
    op: i64,
    args: Vec<Arg>,
}
impl Plan {
    fn compile(&self) -> CalcProgram {
        let mut kinds = vec![self.kind];
        let mut payload = vec![self.op];
        let mut children = vec![self.args.len() as i64];
        let mut longs = Vec::new();
        let mut doubles = Vec::new();
        let mut strings = Vec::new();
        for arg in &self.args {
            children.push(0);
            match arg {
                Arg::Column(index) => {
                    kinds.push(0);
                    payload.push(*index);
                }
                Arg::String(text) => {
                    kinds.push(3);
                    payload.push(strings.len() as i64);
                    strings.push(Some((*text).into()));
                }
                Arg::Integer(value) => {
                    kinds.push(1);
                    payload.push(longs.len() as i64);
                    longs.push(*value);
                }
                Arg::Double(value) => {
                    kinds.push(2);
                    payload.push(doubles.len() as i64);
                    doubles.push(*value);
                }
            }
        }
        CalcProgram::new(kinds, payload, children, longs, doubles, strings)
    }
}
fn plans() -> Vec<Plan> {
    use Arg::*;
    let mut plans = Vec::new();
    let mut add = |name: &str, kind, op, args| {
        plans.push(Plan {
            name: name.into(),
            kind,
            op,
            args,
        })
    };
    for op in [0, 1, 2, 3, 4, 10, 11, 12, 13, 14, 15] {
        add(
            &format!("numeric/op={op}"),
            6,
            op,
            vec![Column(0), Integer(3)],
        );
    }
    for op in [5, 30, 31, 62] {
        add(&format!("numeric/op={op}"), 6, op, vec![Column(0)]);
    }
    for op in [20, 21] {
        add(
            &format!("boolean/op={op}"),
            6,
            op,
            vec![Column(3), Column(3)],
        );
    }
    for op in [22, 32, 33, 34, 35] {
        add(&format!("boolean/op={op}"), 6, op, vec![Column(3)]);
    }
    for op in [50, 51, 52, 54, 59, 60, 61, 95, 96, 97, 98, 99, 143] {
        add(&format!("text/op={op}"), 6, op, vec![Column(2)]);
    }
    add("like", 6, 56, vec![Column(2), String("%alpha%")]);
    add("position", 6, 57, vec![String("alpha"), Column(2)]);
    add(
        "replace",
        6,
        58,
        vec![Column(2), String("alpha"), String("beta")],
    );
    add("repeat", 6, 66, vec![Column(2), Integer(3)]);
    add("concat", 6, 93, vec![Column(2), String("suffix")]);
    add("concat_ws", 6, 94, vec![String("|"), Column(2), Column(2)]);
    for op in [63, 64, 72, 73, 74, 75, 76, 77, 78, 79, 80] {
        add(&format!("floating/op={op}"), 6, op, vec![Column(1)]);
    }
    add("power", 6, 71, vec![Column(1), Double(2.0)]);
    add("round", 6, 84, vec![Column(1), Integer(2)]);
    add("date_format", 6, 86, vec![Column(4), String("%Y-%m-%d")]);
    add(
        "date_format_ltz",
        6,
        90,
        vec![Column(4), String("%Y-%m-%d"), String("America/New_York")],
    );
    add("extract", 6, 89, vec![Column(4), String("year")]);
    add(
        "extract_ltz",
        6,
        91,
        vec![Column(4), String("year"), String("America/New_York")],
    );
    add(
        "regexp_extract",
        6,
        88,
        vec![Column(2), String("(alpha)"), Integer(1)],
    );
    add("timestamp_from_millis", 6, 87, vec![Column(0)]);
    add("interval_scale", 6, 92, vec![Column(0), Integer(3)]);
    add("case", 6, 40, vec![Column(3), Column(2), String("else")]);
    add("narrow_integer", 18, 2, vec![Column(0)]);
    add("widen_float", 11, 5, vec![Column(0)]);
    plans
}
fn expressions(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("engine/calc_expressions");
    for rows in [16, 1024, 16384] {
        for unicode in [false, true] {
            let text = if unicode {
                "é中🙂 alpha".repeat(24)
            } else {
                "alpha xyz".repeat(32)
            };
            let ints = Int64Array::from_iter((0..rows).map(|i| (i % 7 != 0).then_some(i as i64)));
            let input = RecordBatch::try_from_iter(vec![
                ("integer", Arc::new(ints.clone()) as ArrayRef),
                (
                    "float",
                    Arc::new(Float64Array::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some((i % 100) as f64 / 100.0)),
                    )) as ArrayRef,
                ),
                (
                    "text",
                    Arc::new(StringArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
                    )) as ArrayRef,
                ),
                (
                    "boolean",
                    Arc::new(BooleanArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(i % 2 == 0)),
                    )) as ArrayRef,
                ),
                (
                    "timestamp",
                    Arc::new(timestamps_from_millis(&ints)) as ArrayRef,
                ),
            ])
            .unwrap();
            group.throughput(Throughput::Elements(rows as u64));
            for plan in plans() {
                let mut operator = plan.compile();
                operator.run(input.clone());
                let label = format!("{}/{rows}/unicode={unicode}", plan.name);
                let (output, allocations) = measure(|| operator.run(input.clone()));
                assert_eq!(output.num_rows(), rows);
                report(&label, input.columns(), output.columns(), allocations);
                group.bench_function(BenchmarkId::from_parameter(&label), |b| {
                    b.iter(|| {
                        std::hint::black_box(operator.run(std::hint::black_box(input.clone())))
                    })
                });
                let label = format!(
                    "compile_and_first_batch/{}/{rows}/unicode={unicode}",
                    plan.name
                );
                let mut cold = plan.compile();
                let (output, allocations) = measure(|| cold.run(input.clone()));
                report(&label, input.columns(), output.columns(), allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter_batched_ref(
                        || plan.compile(),
                        |operator| std::hint::black_box(operator.run(input.clone())),
                        BatchSize::LargeInput,
                    )
                });
            }
        }
    }
    group.finish();
}
criterion_group!(benches, expressions);
criterion_main!(benches);
