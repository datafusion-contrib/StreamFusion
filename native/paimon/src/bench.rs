//! Uses the production merge loop with in-memory input runs instead of JNI I/O.
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::{collections::VecDeque, sync::Arc};
pub fn merge(
    runs: &[Vec<RecordBatch>],
    batch_rows: usize,
    first_row: bool,
    partial_update: bool,
) -> Vec<RecordBatch> {
    let schema = runs[0][0].schema();
    let output = Arc::new(Schema::new(vec![
        schema.field(3).clone(),
        schema.field(2).clone(),
    ]));
    let options = crate::merge::Options {
        first_row,
        partial_update,
        ..Default::default()
    };
    let mut merger = crate::merge::Merger::new(
        schema,
        output,
        1,
        runs.len(),
        batch_rows,
        usize::MAX,
        options,
    )
    .unwrap();
    let mut pending: Vec<VecDeque<_>> = runs.iter().map(|run| run.clone().into()).collect();
    let mut output = Vec::new();
    while let Some(batch) = merger
        .next(&mut |run| Ok(pending[run].pop_front()))
        .unwrap()
    {
        output.push(batch);
    }
    output
}
