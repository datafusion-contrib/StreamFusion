package io.github.jordepic.streamfusion.operator;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** Metrics shared by native operators that buffer a Flink logical mini-batch. */
final class MiniBatchMetrics {

  enum FlushReason {
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
  private volatile long currentPhysicalBatches;
  private volatile long lastRows;
  private volatile long lastPhysicalBatches;
  private volatile long peakTransientBytes;

  MiniBatchMetrics(MetricGroup metrics) {
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
  }

  void onPhysicalBatch() {
    inputBatches.inc();
  }

  void onSlice(int rows, boolean firstContributionFromPhysicalBatch) {
    inputRows.inc(rows);
    currentRows += rows;
    if (firstContributionFromPhysicalBatch) {
      currentPhysicalBatches++;
    }
  }

  void onPhysicalBatchSplit() {
    physicalBatchSplits.inc();
  }

  void onFlush(FlushReason reason, long emittedRows, long changedKeys, long transientBytes) {
    if (currentRows == 0) {
      return;
    }
    bundles.inc();
    switch (reason) {
      case COUNT -> countFlushes.inc();
      case WATERMARK -> watermarkFlushes.inc();
      case CHECKPOINT -> checkpointFlushes.inc();
      case FINISH -> finishFlushes.inc();
    }
    outputRows.inc(emittedRows);
    touchedKeys.inc(changedKeys);
    lastRows = currentRows;
    lastPhysicalBatches = currentPhysicalBatches;
    peakTransientBytes = Math.max(peakTransientBytes, transientBytes);
    currentRows = 0;
    currentPhysicalBatches = 0;
  }

  void onCancelledChanges(long count) {
    cancelledChanges.inc(count);
  }
}
