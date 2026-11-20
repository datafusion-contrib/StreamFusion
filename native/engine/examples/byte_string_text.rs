//! Prepared Arrow inputs for the byte-backed text measurements.
use arrow::array::{BinaryArray, Int32Array, RecordBatch};
use std::{hint::black_box, sync::Arc, time::Instant};
use streamfusion::bench::CalcProgram;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let iterations: usize = args.get(1).map_or(256, |arg| arg.parse().unwrap());
    let warmups: usize = args.get(2).map_or(5, |arg| arg.parse().unwrap());
    let trials: usize = args.get(3).map_or(7, |arg| arg.parse().unwrap());
    println!("function,distribution,bytes,trial,warmup,ns_per_row,checksum");
    for mode in ["ascii", "unicode", "mixed"] {
        for width in [24, 192, 768] {
            let prefix = if mode == "ascii" {
                "a".repeat(width)
            } else {
                "中".repeat(width / 3)
            };
            let text = (0..1024).map(|row| {
                if row % 31 == 0 {
                    return None;
                }
                let suffix = if row % 2 == 0 { "match" } else { "other" };
                let mut bytes = format!("{prefix}{suffix}").into_bytes();
                if mode == "mixed" && row % 1000 == 1 {
                    bytes[0] = 0xff;
                }
                Some(bytes)
            });
            let text = BinaryArray::from_iter(text);
            let batch = RecordBatch::try_from_iter([("s", Arc::new(text) as _)]).unwrap();
            for (name, mut calc) in [
                (
                    "CHAR_LENGTH",
                    CalcProgram::new(vec![6, 0], vec![52, 0], vec![1, 0], vec![], vec![], vec![]),
                ),
                (
                    "POSITION",
                    CalcProgram::new(
                        vec![6, 3, 0],
                        vec![57, 0, 0],
                        vec![2, 0, 0],
                        vec![],
                        vec![],
                        vec![Some("match".into())],
                    ),
                ),
            ] {
                calc.run(batch.clone());
                for trial in 0..warmups + trials {
                    let mut checksum = 0_i64;
                    let start = Instant::now();
                    for _ in 0..iterations {
                        let output = calc.run(black_box(batch.clone()));
                        let result = output
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        checksum += result.iter().flatten().map(i64::from).sum::<i64>();
                    }
                    let ns =
                        start.elapsed().as_nanos() as f64 / (iterations * batch.num_rows()) as f64;
                    println!(
                        "{name},{mode},{},{trial},{},{ns:.3},{}",
                        width + 5,
                        trial < warmups,
                        black_box(checksum),
                    );
                }
            }
        }
    }
}
