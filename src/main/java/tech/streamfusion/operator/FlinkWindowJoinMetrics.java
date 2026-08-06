package tech.streamfusion.operator;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

/** Exact metric surface registered by Flink's {@code WindowJoinHelper}. */
final class FlinkWindowJoinMetrics {

  private final Counter leftLateRecords;
  private final Counter rightLateRecords;
  private final ProcessingTimeService processingTime;
  private volatile long watermark = Long.MIN_VALUE;
  private long reportedLeft;
  private long reportedRight;

  FlinkWindowJoinMetrics(OperatorMetricGroup metrics, ProcessingTimeService processingTime) {
    this.processingTime = processingTime;
    leftLateRecords = metrics.counter("leftNumLateRecordsDropped");
    rightLateRecords = metrics.counter("rightNumLateRecordsDropped");
    metrics.meter("leftLateRecordsDroppedRate", new MeterView(leftLateRecords));
    metrics.meter("rightLateRecordsDroppedRate", new MeterView(rightLateRecords));
    metrics.gauge(
        "watermarkLatency",
        () -> watermark < 0 ? 0L : processingTime.getCurrentProcessingTime() - watermark);
  }

  void onWatermark(long currentWatermark) {
    watermark = Math.max(watermark, currentWatermark);
  }

  void reportLateRecords(long cumulativeLeft, long cumulativeRight) {
    if (cumulativeLeft > reportedLeft) {
      leftLateRecords.inc(cumulativeLeft - reportedLeft);
      reportedLeft = cumulativeLeft;
    }
    if (cumulativeRight > reportedRight) {
      rightLateRecords.inc(cumulativeRight - reportedRight);
      reportedRight = cumulativeRight;
    }
  }
}
