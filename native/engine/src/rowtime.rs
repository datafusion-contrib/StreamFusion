use crate::*;

/// Max of a rowtime column in epoch millis, or `i64::MIN` when every value is null. The JVM side
/// treats `i64::MIN` as Flink's `NO_TIMESTAMP` sentinel. The planner admits nanosecond timestamps
/// and bigint epoch-millis rowtimes.
pub(crate) fn max_rowtime_millis(batch: &RecordBatch, index: usize) -> i64 {
    let column = batch.column(index);
    match column.data_type() {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("nanosecond timestamp downcast");
            arrow::compute::max(array)
                .map(|ns| ns.div_euclid(1_000_000))
                .unwrap_or(i64::MIN)
        }
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("bigint rowtime downcast");
            arrow::compute::max(array).unwrap_or(i64::MIN)
        }
        other => panic!("unsupported rowtime column type {other}"),
    }
}
