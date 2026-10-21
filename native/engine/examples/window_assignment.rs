//! Match released Flink's RowData assignment gate with prebuilt Arrow and a Rust-side timer.
use arrow::{
    array::{Array, Int64Array},
    ipc::reader::FileReader,
    record_batch::RecordBatch,
};
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion_bridge::timestamp::TimestampColumn;

fn consume(batch: &RecordBatch) -> (usize, i64) {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let timestamps = (1..5)
        .map(|c| TimestampColumn::try_new(batch.column(c).as_ref()).unwrap())
        .collect::<Vec<_>>();
    let mut checksum = 0i64;
    for i in 0..batch.num_rows() {
        checksum = checksum.wrapping_add(ids.value(i));
        for column in &timestamps {
            checksum = checksum.wrapping_add(column.value(i).unwrap().millis());
        }
    }
    (batch.num_rows(), checksum)
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert!(args.len() >= 4, "window_assignment INPUT.arrow TUMBLE|HOP|CUMULATE OFFSET_MS [batch_rows=1024] [warmups=3] [runs=7]");
    let shape = &args[2];
    assert!(matches!(shape.as_str(), "TUMBLE" | "HOP" | "CUMULATE"));
    let offset = args[3].parse::<i64>().unwrap();
    let batch_rows = args
        .get(4)
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(1024);
    let warmups = args
        .get(5)
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(3);
    let runs = args
        .get(6)
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(7);
    let input = FileReader::try_new(File::open(&args[1]).unwrap(), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let batches = (0..input.num_rows())
        .step_by(batch_rows)
        .map(|start| input.slice(start, batch_rows.min(input.num_rows() - start)))
        .collect::<Vec<_>>();
    for trial in 0..warmups + runs {
        let clock = Instant::now();
        let (mut rows, mut checksum) = (0usize, 0i64);
        for batch in &batches {
            let output = streamfusion::bench::assign_fixed_windows(black_box(batch), shape, offset);
            let (n, value) = consume(&output);
            rows += n;
            checksum = checksum.wrapping_add(value);
        }
        black_box((rows, checksum));
        let seconds = clock.elapsed().as_secs_f64();
        println!("{{\"shape\":\"{shape}\",\"trial\":{trial},\"warmup\":{},\"seconds\":{seconds:.9},\"input_rows\":{},\"output_rows\":{rows},\"checksum\":{checksum}}}", trial < warmups, input.num_rows());
    }
}
