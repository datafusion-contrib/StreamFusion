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
}
