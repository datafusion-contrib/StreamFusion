use arrow::array::{BinaryArray, RecordBatch};
use criterion::{BenchmarkId, Criterion, Throughput};
use streamfusion::bench::CalcProgram;
use streamfusion_benchmark_support::{measure, report};

pub(super) fn run(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_expressions");
    for rows in [0, 16, 1024, 16384] {
        for width in [1, 16, 256] {
            for nullable in [false, true] {
                for offset in [0, 1, 5] {
                    for sql in [false, true] {
                        let values: Vec<_> = (0..rows + offset + 1)
                            .map(|i| {
                                (!(nullable && i % 7 == 0)).then(|| {
                                    (0..width)
                                        .map(|j| [0, 0x80, 0xff, i as u8][j % 4])
                                        .collect::<Vec<_>>()
                                })
                            })
                            .collect();
                        let input = super::fixed(&values, width).slice(offset, rows);
                        let expected = BinaryArray::from_iter(
                            values[offset..offset + rows].iter().map(|v| v.as_deref()),
                        );
                        let batch = RecordBatch::try_from_iter([("value", input)]).unwrap();
                        let mut plan = CalcProgram::new(
                            vec![39, 0],
                            vec![if sql { i32::MAX as i64 } else { 0 }, 0],
                            vec![1, 0],
                            vec![],
                            vec![],
                            vec![],
                        );
                        let check = plan.run(batch.clone());
                        assert_eq!(check.num_rows(), rows);
                        assert_eq!(check.column(0).as_ref(), &expected);
                        let (output, allocations) = measure(|| plan.run(batch.clone()));
                        assert_eq!(output.column(0).as_ref(), &expected);
                        let mut label = format!(
                        "fixed_to_variable/{rows}/width={width}/nulls={nullable}/offset={offset}"
                    );
                        if sql {
                            label.push_str("/sql_bytes");
                        }
                        report(&label, batch.columns(), output.columns(), allocations);
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter(|| {
                                std::hint::black_box(plan.run(std::hint::black_box(batch.clone())))
                            })
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
