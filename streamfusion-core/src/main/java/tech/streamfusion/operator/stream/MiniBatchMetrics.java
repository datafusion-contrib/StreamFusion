package tech.streamfusion.operator.stream;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** Metrics shared by native operators that buffer a Flink logical mini-batch. */
@org.apache.flink.annotation.Internal
public final class MiniBatchMetrics {
  public enum FlushReason {
    COUNT,
    WATERMARK,
    CHECKPOINT,
    FINISH
  }

  private final Counter inputRows;
  private final Counter inputBatches;
  private final Counter bundles;
  private final Counter countFlushes;
  private final Counter watermarkFlushes;
  private final Counter checkpointFlushes;
  private final Counter finishFlushes;
  private final Counter outputRows;
  private final Counter touchedKeys;
  private final Counter cancelledChanges;
  private final Counter physicalBatchSplits;

  private volatile long currentRows;
  private volatile long currentKeys;
  private volatile long currentPhysicalBatches;
  private volatile long lastRows;
  private volatile long lastPhysicalBatches;
  private volatile long peakTransientBytes;

  public MiniBatchMetrics(MetricGroup metrics) {
    this(metrics, false);
  }

  public MiniBatchMetrics(MetricGroup metrics, boolean flinkMapBundleSurface) {
    inputRows = metrics.counter("miniBatchInputRows");
    inputBatches = metrics.counter("miniBatchInputBatches");
    bundles = metrics.counter("miniBatchBundles");
    countFlushes = metrics.counter("miniBatchCountFlushes");
    watermarkFlushes = metrics.counter("miniBatchWatermarkFlushes");
    checkpointFlushes = metrics.counter("miniBatchCheckpointFlushes");
    finishFlushes = metrics.counter("miniBatchFinishFlushes");
    outputRows = metrics.counter("miniBatchOutputRows");
    touchedKeys = metrics.counter("miniBatchTouchedKeys");
    cancelledChanges = metrics.counter("miniBatchCancelledChanges");
    physicalBatchSplits = metrics.counter("miniBatchPhysicalBatchSplits");
    metrics.gauge("miniBatchCurrentRows", () -> currentRows);
    metrics.gauge("miniBatchLastRows", () -> lastRows);
    metrics.gauge("miniBatchLastPhysicalBatches", () -> lastPhysicalBatches);
    metrics.gauge("miniBatchPeakTransientBytes", () -> peakTransientBytes);
    if (flinkMapBundleSurface) {
      // Match AbstractMapBundleOperator's names and value types exactly. These are current-bundle
      // gauges, unlike the diagnostic last/touched counters above.
      metrics.gauge("bundleSize", () -> (int) Math.min(Integer.MAX_VALUE, currentRows));
      metrics.gauge(
          "bundleRatio", () -> currentKeys == 0 ? 0.0 : (double) currentRows / currentKeys);
    }
  }

  public void onPhysicalBatch() {
    inputBatches.inc();
  }

  public void onSlice(int rows, boolean firstContributionFromPhysicalBatch) {
    inputRows.inc(rows);
    currentRows += rows;
    if (firstContributionFromPhysicalBatch) {
      currentPhysicalBatches++;
    }
  }

  public void onPhysicalBatchSplit() {
    physicalBatchSplits.inc();
  }

  public void onCurrentKeys(long keys) {
    currentKeys = keys;
  }

  public void onFlush(FlushReason reason, long emittedRows, long changedKeys, long transientBytes) {
    if (currentRows == 0) {
      return;
    }
    bundles.inc();
    switch (reason) {
      case COUNT:
        countFlushes.inc();
        break;
      case WATERMARK:
        watermarkFlushes.inc();
        break;
      case CHECKPOINT:
        checkpointFlushes.inc();
        break;
      case FINISH:
        finishFlushes.inc();
        break;
    }
    outputRows.inc(emittedRows);
    touchedKeys.inc(changedKeys);
    lastRows = currentRows;
    lastPhysicalBatches = currentPhysicalBatches;
    peakTransientBytes = Math.max(peakTransientBytes, transientBytes);
    currentRows = 0;
    currentKeys = 0;
    currentPhysicalBatches = 0;
  }

  public void onCancelledChanges(long count) {
    cancelledChanges.inc(count);
  }
}
