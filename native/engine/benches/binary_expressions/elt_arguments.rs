use super::fixed;
use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::Field;
use criterion::{BenchmarkId, Criterion, Throughput};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use std::sync::Arc;
use streamfusion::bench::binary_elt_function;
use streamfusion_benchmark_support::{measure, report};

pub(super) fn run(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_expressions/elt_arguments");
    for rows in [16, 1024, 16384] {
        group.throughput(Throughput::Elements(rows as u64));
        for width in [1, 16, 256] {
            for nullable in [false, true] {
                let values = (0..=rows)
                    .map(|i| (!(nullable && i % 7 == 0)).then(|| vec![0x80; width]))
                    .collect::<Vec<_>>();
                let input = fixed(&values, width).slice(1, rows);
                let indexes = (0..=rows)
                    .map(|i| match i % 5 {
                        0 => Some(-1),
                        1 => Some(1),
                        2 => Some(2),
                        3 => Some(0),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let dynamic: ArrayRef = Arc::new(Int32Array::from(indexes.clone()));
                let dynamic = dynamic.slice(1, rows);
                for fixed_literal in [false, true] {
                    for literal_null in [false, true] {
                        let literal = (!literal_null).then(|| {
                            let mut bytes = vec![0xff; width];
                            bytes[0] = 0;
                            bytes
                        });
                        let scalar = if fixed_literal {
                            ScalarValue::FixedSizeBinary(width as i32, literal.clone())
                        } else {
                            ScalarValue::Binary(literal.clone())
                        };
                        // This array is a diagnostic control; expansion is deliberately outside timing.
                        let expanded = scalar.to_array_of_size(rows).unwrap();
                        for (index_name, constant) in [
                            ("dynamic", None),
                            ("first", Some(Some(1))),
                            ("literal", Some(Some(2))),
                            ("invalid", Some(Some(0))),
                            ("null", Some(None)),
                        ] {
                            let expected = (1..=rows)
                                .map(|i| match constant.unwrap_or(indexes[i]) {
                                    Some(1) => values[i].clone(),
                                    Some(2) => literal.clone(),
                                    _ => None,
                                })
                                .collect::<Vec<_>>();
                            let expected = fixed(&expected, width);
                            for representation in ["scalar", "expanded_control"] {
                                let index = constant.map_or_else(
                                    || ColumnarValue::Array(dynamic.clone()),
                                    |value| ColumnarValue::Scalar(ScalarValue::Int32(value)),
                                );
                                let value = if representation == "scalar" {
                                    ColumnarValue::Scalar(scalar.clone())
                                } else {
                                    ColumnarValue::Array(expanded.clone())
                                };
                                let args = vec![index, ColumnarValue::Array(input.clone()), value];
                                let fields = args
                                    .iter()
                                    .map(|value| {
                                        Arc::new(Field::new("arg", value.data_type(), true))
                                    })
                                    .collect::<Vec<_>>();
                                let scalars = args
                                    .iter()
                                    .map(|value| match value {
                                        ColumnarValue::Scalar(value) => Some(value),
                                        _ => None,
                                    })
                                    .collect::<Vec<_>>();
                                let udf = binary_elt_function(3, width as i32);
                                let field = udf
                                    .return_field_from_args(ReturnFieldArgs {
                                        arg_fields: &fields,
                                        scalar_arguments: &scalars,
                                    })
                                    .unwrap();
                                let options = Arc::new(ConfigOptions::new());
                                let invoke = || {
                                    udf.invoke_with_args(ScalarFunctionArgs {
                                        args: args.clone(),
                                        arg_fields: fields.clone(),
                                        number_rows: rows,
                                        return_field: field.clone(),
                                        config_options: options.clone(),
                                    })
                                    .unwrap()
                                };
                                let warm = invoke().to_array(rows).unwrap();
                                assert_eq!(warm.as_ref(), expected.as_ref());
                                assert_eq!(warm.data_type(), field.data_type());
                                let label = format!("elt_arguments/{representation}/{rows}/width={width}/nulls={nullable}/fixed_literal={fixed_literal}/literal_null={literal_null}/index={index_name}");
                                let (output, allocations) = measure(invoke);
                                assert_eq!(
                                    output.to_array(rows).unwrap().as_ref(),
                                    expected.as_ref()
                                );
                                let inputs = args
                                    .iter()
                                    .filter_map(|value| match value {
                                        ColumnarValue::Array(array) => Some(array.clone()),
                                        _ => None,
                                    })
                                    .collect::<Vec<_>>();
                                let outputs = match output {
                                    ColumnarValue::Array(array) => vec![array],
                                    ColumnarValue::Scalar(_) => vec![],
                                };
                                report(&label, &inputs, &outputs, allocations);
                                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                                    b.iter(|| std::hint::black_box(invoke()))
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    group.finish();
    run_scalars(c);
}

fn run_scalars(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_expressions/elt_all_scalar");
    group.throughput(Throughput::Elements(1));
    for width in [0, 1, 16] {
        for value_count in [1, 3] {
            for fixed_literal in [false, true] {
                for nullable in [false, true] {
                    let values = (0..value_count)
                        .map(|i| {
                            (!(nullable && i % 2 == 0)).then(|| {
                                let len = if width == 0 {
                                    i + usize::from(fixed_literal)
                                } else {
                                    width as usize
                                };
                                let mut bytes = vec![0xff; len];
                                if let Some(first) = bytes.first_mut() {
                                    *first = 0;
                                }
                                bytes
                            })
                        })
                        .collect::<Vec<_>>();
                    for index in [Some(-1), Some(0), Some(1), Some(2), Some(3), Some(4), None] {
                        let expected = index
                            .filter(|&index| index > 0)
                            .and_then(|index| values.get(index as usize - 1))
                            .cloned()
                            .flatten();
                        let expected = if width > 0 {
                            ScalarValue::FixedSizeBinary(width, expected)
                        } else {
                            ScalarValue::Binary(expected)
                        };
                        let mut args = vec![ColumnarValue::Scalar(ScalarValue::Int32(index))];
                        args.extend(values.iter().enumerate().map(|(i, value)| {
                            ColumnarValue::Scalar(if fixed_literal {
                                let input_width = if width == 0 { i as i32 + 1 } else { width };
                                ScalarValue::FixedSizeBinary(input_width, value.clone())
                            } else {
                                ScalarValue::Binary(value.clone())
                            })
                        }));
                        let fields = args
                            .iter()
                            .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                            .collect::<Vec<_>>();
                        let scalars = args
                            .iter()
                            .map(|arg| match arg {
                                ColumnarValue::Scalar(value) => Some(value),
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>();
                        let udf = binary_elt_function(args.len(), width);
                        let field = udf
                            .return_field_from_args(ReturnFieldArgs {
                                arg_fields: &fields,
                                scalar_arguments: &scalars,
                            })
                            .unwrap();
                        let options = Arc::new(ConfigOptions::new());
                        let invoke = || {
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: fields.clone(),
                                // An all-scalar expression stays scalar even in a larger batch.
                                number_rows: 1024,
                                return_field: field.clone(),
                                config_options: options.clone(),
                            })
                            .unwrap()
                        };
                        let label = format!("elt_all_scalar/width={width}/values={value_count}/fixed_literal={fixed_literal}/nulls={nullable}/index={index:?}");
                        let (output, allocations) = measure(invoke);
                        match output {
                            ColumnarValue::Scalar(value) => {
                                assert_eq!(value, expected, "{label}");
                                assert_eq!(&value.data_type(), field.data_type());
                            }
                            ColumnarValue::Array(_) => {
                                panic!("all-scalar ELT returned an array: {label}")
                            }
                        }
                        report(&label, &[], &[], allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter(|| std::hint::black_box(invoke()))
                        });
                    }
                }
            }
        }
    }
    group.finish();
}
