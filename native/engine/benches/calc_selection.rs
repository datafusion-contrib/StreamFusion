//! Diagnostic prototype for #249; the selected-prefix path is not an execution engine contract.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray, StringBuilder};
use arrow::compute::filter;
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use streamfusion::bench::flink_scalar_function;

struct CountAlloc;
static COUNT: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
#[global_allocator]
static ALLOCATOR: CountAlloc = CountAlloc;
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT.load(Relaxed) {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size(), Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNT.load(Relaxed) {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(size, Relaxed);
        }
        System.realloc(ptr, layout, size)
    }
}

fn current(input: &StringArray, mask: &BooleanArray) -> ArrayRef {
    let selected = mask.true_count();
    if selected == 0 {
        return Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
    }
    let filtered: ArrayRef = if selected == input.len() {
        Arc::new(input.clone())
    } else {
        filter(input, mask).unwrap()
    };
    static UDF: OnceLock<datafusion::logical_expr::ScalarUDF> = OnceLock::new();
    static OPTIONS: OnceLock<Arc<ConfigOptions>> = OnceLock::new();
    static FIELDS: OnceLock<Vec<Arc<Field>>> = OnceLock::new();
    static RETURN: OnceLock<Arc<Field>> = OnceLock::new();
    let udf = UDF.get_or_init(|| flink_scalar_function(125, 3));
    let args = vec![
        ColumnarValue::Array(filtered),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(32))),
    ];
    let fields = FIELDS
        .get_or_init(|| {
            vec![
                Arc::new(Field::new("payload", DataType::Utf8, true)),
                Arc::new(Field::new("start", DataType::Int32, false)),
                Arc::new(Field::new("length", DataType::Int32, false)),
            ]
        })
        .clone();
    udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields: fields,
        number_rows: selected,
        return_field: RETURN
            .get_or_init(|| Arc::new(Field::new("result", DataType::Utf8, true)))
            .clone(),
        config_options: OPTIONS
            .get_or_init(|| Arc::new(ConfigOptions::new()))
            .clone(),
    })
    .unwrap()
    .into_array(selected)
    .unwrap()
}

fn selected_prefix(input: &StringArray, mask: &BooleanArray) -> ArrayRef {
    let selected = mask.true_count();
    if selected == 0 {
        return Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
    }
    let mut result = StringBuilder::with_capacity(selected, selected * 32);
    for row in mask.values().set_indices() {
        if mask.is_null(row) {
            continue;
        }
        if input.is_null(row) {
            result.append_null();
        } else {
            let value = input.value(row);
            let end = if value.is_ascii() {
                value.len().min(32)
            } else {
                value
                    .char_indices()
                    .nth(32)
                    .map_or(value.len(), |(offset, _)| offset)
            };
            result.append_value(&value[..end]);
        }
    }
    Arc::new(result.finish())
}

fn filtered_prefix(input: &StringArray, mask: &BooleanArray) -> ArrayRef {
    if mask.true_count() == 0 {
        return selected_prefix(input, mask);
    }
    let filtered = if mask.true_count() == input.len() {
        Arc::new(input.clone()) as ArrayRef
    } else {
        filter(input, mask).unwrap()
    };
    let input = filtered.as_any().downcast_ref::<StringArray>().unwrap();
    let all = BooleanArray::from(vec![true; input.len()]);
    selected_prefix(input, &all)
}

fn diagnostics(c: &mut Criterion) {
    const ROWS: usize = 4096;
    eprintln!("ALLOCATION_HEADER,path,bytes,unicode,selectivity,allocations,requested_bytes,selected_input_bytes,output_bytes,copied_payload_bytes");
    let mut group = c.benchmark_group("calc_selection_prefix");
    group
        .sample_size(20)
        .nresamples(1000)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(1));
    for bytes in [32, 256, 4096] {
        for unicode in [false, true] {
            let text = if unicode {
                "é中🙂".repeat(bytes / 9 + 1)
            } else {
                "x".repeat(bytes)
            };
            let input =
                StringArray::from_iter((0..ROWS).map(|i| (i % 7 != 0).then_some(text.as_str())));
            for percent in [0, 1, 10, 50, 100] {
                let mask = BooleanArray::from_iter((0..ROWS).map(|i| {
                    if percent != 100 && i % 13 == 0 {
                        None
                    } else {
                        Some(i % 100 < percent)
                    }
                }));
                let baseline = current(&input, &mask);
                let candidate = selected_prefix(&input, &mask);
                assert_eq!(baseline.to_data(), candidate.to_data());
                let label = format!("{bytes}/unicode={unicode}/select={percent}");
                let selected_input_bytes: usize = (0..ROWS)
                    .filter(|&i| mask.is_valid(i) && mask.value(i) && input.is_valid(i))
                    .map(|i| input.value(i).len())
                    .sum();
                for (name, run) in [
                    (
                        "pruned_filter",
                        current as fn(&StringArray, &BooleanArray) -> ArrayRef,
                    ),
                    ("bounded_filtered_prefix", filtered_prefix),
                    ("selected_prefix", selected_prefix),
                ] {
                    ALLOCS.store(0, Relaxed);
                    BYTES.store(0, Relaxed);
                    COUNT.store(true, Relaxed);
                    let output = run(&input, &mask);
                    COUNT.store(false, Relaxed);
                    assert_eq!(output.to_data(), baseline.to_data());
                    let output_bytes = output
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .values()
                        .len();
                    let copied = output_bytes
                        + if name != "selected_prefix" && mask.true_count() < ROWS {
                            selected_input_bytes
                        } else {
                            0
                        };
                    eprintln!(
                        "ALLOCATION,{name},{bytes},{unicode},{percent},{},{},{selected_input_bytes},{output_bytes},{copied}",
                        ALLOCS.load(Relaxed),
                        BYTES.load(Relaxed)
                    );
                    group.bench_function(BenchmarkId::new(name, &label), |b| {
                        b.iter(|| {
                            std::hint::black_box(run(
                                std::hint::black_box(&input),
                                std::hint::black_box(&mask),
                            ))
                        })
                    });
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, diagnostics);
criterion_main!(benches);
