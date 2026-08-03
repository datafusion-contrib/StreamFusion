package tech.streamfusion.operator;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Keeps Flink's record counters counting rows for operators whose record is a batch of them.
 *
 * <p>The runtime charges exactly one record per {@code collect}, which is right for a rowwise
 * operator and wrong by the batch size for a columnar one: a native operator moving four thousand
 * rows at a time reports throughput about four thousand times too low, and every derived number —
 * the web UI's rate, backpressure attribution, any alert built on them — is off by the same factor.
 * Emitting through here charges the batch's remaining rows so the totals mean what they say.
 */
final class ColumnarRecordMetrics {

  private ColumnarRecordMetrics() {}

  /** Emits a batch and charges its rows, not the single record the runtime would count. */
  static void emit(
      Output<StreamRecord<ArrowBatch>> output, OperatorMetricGroup metrics, ArrowBatch batch) {
    // rowCount(), not root(): root() is a hand-off and would spend a consumer's share.
    int rows = batch.rowCount();
    output.collect(new StreamRecord<>(batch));
    countRows(metrics.getIOMetricGroup().getNumRecordsOutCounter(), rows);
  }

  /** Charges an ingested batch's rows; call once per record the operator is handed. */
  static void countIngested(OperatorMetricGroup metrics, int rows) {
    countRows(metrics.getIOMetricGroup().getNumRecordsInCounter(), rows);
  }

  // The runtime already charged one for the record itself, so only the remainder is owed. An empty
  // batch is still one record to the runtime; leaving that single count is closer than going
  // negative, and empty batches are not on any path worth distorting the totals for.
  private static void countRows(Counter counter, int rows) {
    if (rows > 1) {
      counter.inc(rows - 1L);
    }
  }
}
