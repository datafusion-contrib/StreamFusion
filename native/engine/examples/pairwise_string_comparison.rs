//! Prepared Arrow control for cache-sensitive Flink string ordering.
use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch};
use arrow::datatypes::{Field, Schema};
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;
use streamfusion_bridge::string_cache::{string_cache_array, string_cache_field};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let iterations: usize = args.get(1).map_or(128, |arg| arg.parse().unwrap());
    let rounds: usize = args.get(2).map_or(7, |arg| arg.parse().unwrap());
    let equality = args.get(3).is_some_and(|arg| arg == "eq");
    let ascii = args.get(4).is_some_and(|arg| arg == "ascii");
    println!("engine,mode,shape,width,trial,warmup,ns_per_row,checksum");
    for mode in ["bytes", "utf16", "disjoint", "mixed"] {
        for shape in ["early", "prefix"] {
            for width in [24, 192] {
                let batch = input(mode, shape, width, equality, ascii);
                // STRING_COMPARISON over two INPUT_REF nodes, with either EQUAL or LESS_THAN.
                let mut calc = CalcProgram::new(
                    vec![49, 0, 0],
                    vec![if equality { 14 } else { 12 }, 0, 1],
                    vec![2, 0, 0],
                    vec![],
                    vec![],
                    vec![],
                );
                calc.run(batch.clone());
                for trial in 0..rounds + 5 {
                    let mut checksum = 0_i64;
                    let start = Instant::now();
                    for _ in 0..iterations {
                        let output = calc.run(black_box(batch.clone()));
                        let result = output
                            .column(0)
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap();
                        checksum += result.true_count() as i64;
                    }
                    let per_row =
                        start.elapsed().as_nanos() as f64 / (iterations * batch.num_rows()) as f64;
                    println!(
                        "native,{mode},{shape},{width},{trial},{},{per_row:.3},{}",
                        trial < 5,
                        black_box(checksum)
                    );
                }
            }
        }
    }
}

fn input(mode: &str, shape: &str, width: usize, equality: bool, ascii: bool) -> RecordBatch {
    let mut values = [Vec::new(), Vec::new()];
    let mut caches = [Vec::new(), Vec::new()];
    let (filler, first, second) = if ascii {
        ("m".repeat(width), "a", "z")
    } else {
        ("中".repeat(width / 3), "\u{10000}", "\u{e000}")
    };
    for row in 0..1024 {
        for column in 0..2 {
            if row % 31 == 0 && column == 0 {
                values[column].push(None);
                caches[column].push(None);
                continue;
            }
            let differs = column == 1 && (!equality || row % 2 != 0);
            let distinctive = if (row % 8 == 1) == differs {
                first
            } else {
                second
            };
            let value = if shape == "early" {
                format!("{distinctive}{filler}")
            } else {
                format!("{filler}{distinctive}")
            };
            let cached = match mode {
                "utf16" => true,
                "disjoint" => row % 2 == column,
                "mixed" => row % 4 & (1 << column) == 0,
                _ => false,
            };
            caches[column].push(cached.then(|| value.encode_utf16().collect::<Vec<_>>()));
            values[column].push(Some(value.into_bytes()));
        }
    }
    let mut arrays: Vec<ArrayRef> = values
        .into_iter()
        .map(|v| Arc::new(BinaryArray::from_iter(v)) as ArrayRef)
        .collect();
    let mut fields = vec![
        Field::new("a", arrays[0].data_type().clone(), true),
        Field::new("b", arrays[1].data_type().clone(), true),
    ];
    for (index, cache) in caches.iter().enumerate() {
        if cache.iter().any(Option::is_some) {
            let values = string_cache_array(cache.iter().map(Option::as_deref));
            fields.push(string_cache_field(index, values.data_type().clone()));
            arrays.push(values);
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}
