use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int32Array, RecordBatch,
    StringArray,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn fixed(values: &[Option<Vec<u8>>], width: usize) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), width as i32);
    for value in values {
        match value {
            Some(value) => builder.append_value(value).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
fn cast_plan(width: usize) -> CalcProgram {
    CalcProgram::new(
        vec![39, 0],
        vec![-(width as i64), 0],
        vec![1, 0],
        vec![],
        vec![],
        vec![],
    )
}
fn run(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("binary_expressions");
    for rows in [16, 1024, 16384] {
        for width in [1, 16, 256] {
            for nullable in [false, true] {
                let text = "é中\0abcdef".repeat(width / 8 + 1);
                let strings: Vec<_> = (0..rows + 1)
                    .map(|i| {
                        if nullable && i % 7 == 0 {
                            None
                        } else if i % 3 == 0 {
                            Some("")
                        } else {
                            Some(text.as_str())
                        }
                    })
                    .collect();
                let variable: Vec<_> = strings
                    .iter()
                    .map(|v| v.map(|s| s.as_bytes().to_vec()))
                    .collect();
                let fixed_values: Vec<_> = variable
                    .iter()
                    .map(|v| v.as_ref().map(|_| vec![0x80; width + 3]))
                    .collect();
                for (name, input, values) in [
                    (
                        "string_cast",
                        Arc::new(StringArray::from(strings)) as ArrayRef,
                        variable.clone(),
                    ),
                    (
                        "binary_cast",
                        Arc::new(BinaryArray::from_iter(
                            variable.iter().map(|v| v.as_deref()),
                        )) as ArrayRef,
                        variable,
                    ),
                    ("fixed_cast", fixed(&fixed_values, width + 3), fixed_values),
                ] {
                    let input = input.slice(1, rows);
                    let expected: Vec<_> = values[1..]
                        .iter()
                        .map(|v| {
                            v.as_ref().map(|v| {
                                let mut bytes = v[..v.len().min(width)].to_vec();
                                bytes.resize(width, 0);
                                bytes
                            })
                        })
                        .collect();
                    let batch = RecordBatch::try_from_iter([("value", input)]).unwrap();
                    let mut plan = cast_plan(width);
                    let warm = plan.run(batch.clone());
                    assert_eq!(warm.column(0).as_ref(), fixed(&expected, width).as_ref());
                    let label = format!("{name}/{rows}/width={width}/nulls={nullable}");
                    let (out, allocation) = measure(|| plan.run(batch.clone()));
                    report(&label, batch.columns(), out.columns(), allocation);
                    group.throughput(Throughput::Elements(rows as u64));
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(plan.run(std::hint::black_box(batch.clone())))
                        })
                    });
                }
                let a: Vec<_> = (0..rows + 1)
                    .map(|i| (!(nullable && i % 7 == 0)).then(|| vec![0x80; width]))
                    .collect();
                let b: Vec<_> = (0..rows + 1)
                    .map(|i| (!(nullable && i % 11 == 0)).then(|| vec![0; width]))
                    .collect();
                let indexes: Vec<_> = (0..rows + 1)
                    .map(|i| match i % 5 {
                        0 => Some(-1),
                        1 => Some(1),
                        2 => Some(2),
                        3 => Some(3),
                        _ => None,
                    })
                    .collect();
                let expected: Vec<_> = (1..=rows)
                    .map(|i| match indexes[i] {
                        Some(1) => a[i].clone(),
                        Some(2) => b[i].clone(),
                        _ => None,
                    })
                    .collect();
                let batch = RecordBatch::try_from_iter([
                    ("index", Arc::new(Int32Array::from(indexes)) as ArrayRef),
                    ("a", fixed(&a, width)),
                    ("b", fixed(&b, width)),
                ])
                .unwrap()
                .slice(1, rows);
                let mut plan = CalcProgram::new(
                    vec![40, 0, 0, 0],
                    vec![width as i64, 0, 1, 2],
                    vec![3, 0, 0, 0],
                    vec![],
                    vec![],
                    vec![],
                );
                let warm = plan.run(batch.clone());
                assert_eq!(warm.column(0).as_ref(), fixed(&expected, width).as_ref());
                assert_eq!(
                    warm.column(0)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap()
                        .value_length(),
                    width as i32
                );
                let label = format!("elt/{rows}/width={width}/nulls={nullable}");
                let (out, allocation) = measure(|| plan.run(batch.clone()));
                report(&label, batch.columns(), out.columns(), allocation);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| std::hint::black_box(plan.run(std::hint::black_box(batch.clone()))))
                });
            }
        }
    }
    group.finish();
}
#[path = "binary_expressions/elt_arguments.rs"]
mod elt_arguments;
#[path = "binary_expressions/fixed_to_variable.rs"]
mod fixed_to_variable;
criterion_group!(benches, run, elt_arguments::run, fixed_to_variable::run);
criterion_main!(benches);
