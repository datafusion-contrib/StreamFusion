//! Prepared Arrow enters before timing; both production aggregate stages and consumption run in Rust.
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, Int8Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use serde::Deserialize;
use std::{fs::File, hint::black_box, time::Instant};
use streamfusion::bench::{GroupBy, LocalGroupBy};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalConfig {
    aggregate_kinds: Vec<i64>,
    value_types: Vec<i64>,
    value_columns: Vec<i64>,
    key_columns: Vec<usize>,
    filter_columns: Vec<i64>,
    distinct_view_sources: Vec<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GlobalConfig {
    aggregate_kinds: Vec<i64>,
    value_types: Vec<i64>,
    value_columns: Vec<i64>,
    key_columns: Vec<usize>,
    count_columns: Vec<i64>,
    distinct_view_columns: Vec<i64>,
}

#[derive(Deserialize)]
struct Config {
    rows: usize,
    bundle: usize,
    local: LocalConfig,
    global: GlobalConfig,
    expected_rows: usize,
    expected_checksum: i64,
}

fn bits(column: &ArrayRef, row: usize) -> i64 {
    if column.is_null(row) {
        return 0x9e3779b97f4a7c15_u64 as i64;
    }
    match column.data_type() {
        DataType::Int32 => column
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row) as i64,
        DataType::Int64 => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row),
        DataType::Float32 => column
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .to_bits() as i64,
        DataType::Float64 => column
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_bits() as i64,
        other => panic!("unexpected aggregate output: {other:?}"),
    }
}

fn consume(batch: &RecordBatch, digest: &mut (usize, i64)) {
    if batch.num_rows() == 0 {
        return;
    }
    let kinds = batch
        .column_by_name("$row_kind$")
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    for row in 0..batch.num_rows() {
        let mut hash = kinds.value(row) as i64;
        for column in &batch.columns()[..batch.num_columns() - 1] {
            hash = hash.wrapping_mul(31).wrapping_add(bits(column, row));
        }
        digest.0 += 1;
        digest.1 = digest.1.wrapping_add(hash);
    }
    black_box(&mut *digest);
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "usage: distinct_partials_admission INPUT.arrow CONFIG.json [warmups=3] [runs=7]"
    );
    let config: Config = serde_json::from_reader(File::open(&args[2]).unwrap()).unwrap();
    let warmups: usize = args.get(3).map_or(3, |value| value.parse().unwrap());
    let runs: usize = args.get(4).map_or(7, |value| value.parse().unwrap());
    let mut reader = FileReader::try_new(File::open(&args[1]).unwrap(), None).unwrap();
    let input = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    assert_eq!(input.num_rows(), config.rows);
    let batches: Vec<_> = (0..input.num_rows())
        .step_by(config.bundle)
        .map(|row| input.slice(row, config.bundle.min(input.num_rows() - row)))
        .collect();
    for trial in 0..warmups + runs {
        let mut local = LocalGroupBy::filtered(
            config.local.aggregate_kinds.clone(),
            config.local.value_types.clone(),
            config.local.value_columns.clone(),
            config.local.filter_columns.clone(),
            config.local.key_columns.clone(),
            config.local.distinct_view_sources.clone(),
        )
        .with_budget(1 << 30);
        let mut global = GroupBy::distinct_partial_merge(
            config.global.aggregate_kinds.clone(),
            config.global.value_types.clone(),
            config.global.value_columns.clone(),
            config.global.key_columns.clone(),
            config.global.count_columns.clone(),
            config.global.distinct_view_columns.clone(),
        )
        .with_budget(1 << 30);
        let mut pending = 0;
        let mut digest = (0, 0_i64);
        let start = Instant::now();
        for batch in &batches {
            local.update(batch);
            let partial = local.flush();
            let mut row = 0;
            while row < partial.num_rows() {
                let count = (config.bundle - pending).min(partial.num_rows() - row);
                consume(&global.update(&partial.slice(row, count)), &mut digest);
                pending += count;
                row += count;
                if pending == config.bundle {
                    consume(&global.flush(), &mut digest);
                    pending = 0;
                }
            }
        }
        consume(&global.flush(), &mut digest);
        let seconds = start.elapsed().as_secs_f64();
        assert_eq!(digest, (config.expected_rows, config.expected_checksum));
        println!("[partials-admission-rust] rows={} bundle={} trial={} warmup={} seconds={:.9} output_rows={} checksum={}",
            config.rows, config.bundle, trial, trial < warmups, seconds, digest.0, digest.1);
    }
}
