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
            ("date", 8),
            ("time", 9),
            ("boolean", 10),
            ("timestamp", 7),
            ("string8", 3),
            ("string256", 3),
            ("array_int", 100),
            ("array_double", 100),
            ("array_string", 100),
            ("interval_month", 100),
            ("interval_day", 100),
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
                "date" => Arc::new(arrow::array::Date32Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i32))
                        .collect::<Vec<_>>(),
                )),
                "time" => Arc::new(arrow::array::Time32MillisecondArray::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| (v + 128) as i32))
                        .collect::<Vec<_>>(),
                )),
                "boolean" => Arc::new(BooleanArray::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v % 2 == 0))
                        .collect::<Vec<_>>(),
                )),
                "timestamp" => Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                    values.iter().map(|v| {
                        v.map(|v| {
                            streamfusion_bridge::timestamp::TimestampValue::new(
                                -1,
                                (v + 128) as u32,
                            )
                            .unwrap()
                        })
                    }),
                )),
                "array_int" => Arc::new(arrow::array::ListArray::from_iter_primitive::<
                    arrow::datatypes::Int32Type,
                    _,
                    _,
                >(
                    values.iter().map(|v| v.map(|v| vec![Some(v as i32), None])),
                )),
                "array_double" => Arc::new(arrow::array::ListArray::from_iter_primitive::<
                    arrow::datatypes::Float64Type,
                    _,
                    _,
                >(
                    values.iter().map(|v| v.map(|v| vec![Some(v as f64), None])),
                )),
                "array_string" => {
                    let mut builder =
                        arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
                    for value in &values {
                        if let Some(value) = value {
                            builder.values().append_value(format!("{value:08}"));
                            builder.values().append_null();
                        }
                        builder.append(value.is_some());
                    }
                    Arc::new(builder.finish())
                }
                "interval_month" => Arc::new(arrow::array::IntervalYearMonthArray::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i32))
                        .collect::<Vec<_>>(),
                )),
                "interval_day" => Arc::new(arrow::array::IntervalDayTimeArray::from(
                    values
                        .iter()
                        .map(|v| {
                            v.map(|v| {
                                arrow::datatypes::IntervalDayTimeType::make_value(v as i32, 7)
                            })
                        })
                        .collect::<Vec<_>>(),
                )),
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
            let mut fixture =
                LocalGroupBy::filtered(vec![7], vec![code], vec![1], vec![-1], vec![0], vec![0]);
            fixture.update(&batch);
            let output = fixture.flush();
            assert_eq!(output.num_rows(), 16);
            let DataType::List(entries) = output.column(2).data_type() else {
                panic!("expected a typed distinct view");
            };
            let DataType::Struct(fields) = entries.data_type() else {
                panic!("expected value/count entries");
            };
            assert_eq!(fields[0].data_type(), batch.column(1).data_type());
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
            if code == 100 {
                let keys = output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..output.num_rows() {
                    let expected: std::collections::HashSet<_> = (0..ROWS)
                        .filter(|i| *i % 16 == keys.value(row) as usize && *i % 7 != 0)
                        .map(|i| (i / 16) % cardinality)
                        .collect();
                    assert_eq!(counts.value(row), expected.len() as i64);
                }
                continue;
            }
            let single = RecordBatch::try_new(
                batch.schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
                    batch.column(1).clone(),
                ],
            )
            .unwrap();
            for mini_batch in [false, true] {
                let create_global = || {
                    if mini_batch {
                        GroupBy::mini_batch(vec![14], vec![code], vec![1], vec![0])
                    } else {
                        GroupBy::new(vec![14], vec![code], vec![1], vec![0])
                    }
                };
                let execute = |global: &mut GroupBy| {
                    let result = global.update(&single);
                    if mini_batch {
                        global.flush()
                    } else {
                        result
                    }
                };
                let output = execute(&mut create_global());
                assert_eq!(output.num_rows(), ROWS);
                assert_eq!(output.column(0), single.column(0));
                assert_eq!(output.column(1), single.column(1));
                let mode = if mini_batch {
                    "mini_batch"
                } else {
                    "immediate"
                };
                group.bench_function(
                    format!("global_single/{mode}/{name}/cardinality{cardinality}"),
                    |b| {
                        b.iter_batched_ref(
                            create_global,
                            |global| black_box(execute(global)),
                            BatchSize::SmallInput,
                        )
                    },
                );
            }
            let create_single =
                || LocalGroupBy::new(vec![14, 21], vec![code; 2], vec![1; 2], vec![0]);
            let mut checked = create_single();
            checked.update(&single);
            let partials = checked.flush();
            assert_eq!(partials.num_rows(), ROWS);
            assert_eq!(partials.column(1), single.column(1));
            let counts = partials
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!(counts.iter().all(|count| count == Some(1)));
            group.bench_function(
                format!("local_single/{name}/cardinality{cardinality}"),
                |b| {
                    b.iter_batched_ref(
                        create_single,
                        |local| {
                            local.update(black_box(&single));
                            black_box(local.flush());
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}
