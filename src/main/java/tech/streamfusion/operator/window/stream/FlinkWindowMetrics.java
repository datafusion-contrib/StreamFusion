package tech.streamfusion.operator.window.stream;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

/** Exact metric surface shared by Flink's window aggregate/rank/deduplicate operators. */
@org.apache.flink.annotation.Internal
public final class FlinkWindowMetrics {

  private final Counter lateRecords;
  private final ProcessingTimeService processingTime;
  private volatile long watermark = Long.MIN_VALUE;
  private long reportedLateRecords;

  public FlinkWindowMetrics(OperatorMetricGroup metrics, ProcessingTimeService processingTime) {
    this.processingTime = processingTime;
    lateRecords = metrics.counter("numLateRecordsDropped");
    metrics.meter("lateRecordsDroppedRate", new MeterView(lateRecords));
    metrics.gauge(
        "watermarkLatency",
        () -> watermark < 0 ? 0L : processingTime.getCurrentProcessingTime() - watermark);
  }

  public void onWatermark(long currentWatermark) {
    watermark = Math.max(watermark, currentWatermark);
  }

  /** Publishes a cumulative native count without double-counting repeated samples. */
  public void reportLateRecords(long cumulativeLateRecords) {
    if (cumulativeLateRecords > reportedLateRecords) {
      lateRecords.inc(cumulativeLateRecords - reportedLateRecords);
      reportedLateRecords = cumulativeLateRecords;
    }
  }
}
