use super::*;
use arrow::array::{Decimal128Array, Int16Array, Int32Array};
use streamfusion::bench::GroupBy;

pub fn bench_typed_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("typed_distinct");
    group.throughput(Throughput::Elements(ROWS as u64));
    for cardinality in [4, 256] {
        for (name, code) in [
            ("bigint", 0),
            ("int", 2),
            ("smallint", 4),
            ("tinyint", 5),
            ("decimal", 4002),
            ("string8", 3),
            ("string256", 3),
        ] {
            let values: Vec<Option<i64>> = (0..ROWS)
                .map(|i| (i % 7 != 0).then_some(((i / 16) % cardinality) as i64 - 128))
                .collect();
            let value: ArrayRef = match name {
                "bigint" => Arc::new(Int64Array::from(values)),
                "int" => Arc::new(Int32Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i32))
                        .collect::<Vec<_>>(),
                )),
                "smallint" => Arc::new(Int16Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i16))
                        .collect::<Vec<_>>(),
                )),
                "tinyint" => Arc::new(Int8Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i8))
                        .collect::<Vec<_>>(),
                )),
                "decimal" => Arc::new(
                    Decimal128Array::from(
                        values
                            .iter()
                            .map(|v| v.map(|v| i128::from(v) * 100_000_000_000))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
                ),
                _ => {
                    let width = if name == "string8" { 8 } else { 256 };
                    Arc::new(StringArray::from(
                        values
                            .iter()
                            .map(|v| v.map(|v| format!("{v:0width$}")))
                            .collect::<Vec<_>>(),
                    ))
                }
            };
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("v", value.data_type().clone(), true),
                ])),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS).map(|i| (i % 16) as i64),
                    )),
                    value,
                ],
            )
            .unwrap();
            for local in [false, true] {
                let id = format!(
                    "{}/{name}/cardinality{cardinality}",
                    if local { "local" } else { "single" }
                );
                group.bench_function(id, |b| {
                    if local {
                        b.iter_batched(
                            || {
                                LocalGroupBy::filtered(
                                    vec![7],
                                    vec![code],
                                    vec![1],
                                    vec![-1],
                                    vec![0],
                                    vec![0],
                                )
                            },
                            |mut agg| {
                                agg.update(black_box(&batch));
                                black_box(agg.flush());
                            },
                            BatchSize::SmallInput,
                        );
                    } else {
                        b.iter_batched(
                            || GroupBy::new(vec![7], vec![code], vec![1], vec![0]),
                            |mut agg| black_box(agg.update(black_box(&batch))),
                            BatchSize::SmallInput,
                        );
                    }
                });
            }
        }
    }
    group.finish();
}
