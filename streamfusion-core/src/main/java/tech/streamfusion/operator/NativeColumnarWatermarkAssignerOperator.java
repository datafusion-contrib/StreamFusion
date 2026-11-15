package tech.streamfusion.operator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Columnar event-time watermark assigner: the Arrow-batch analog of the host's {@link
 * org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator} with a bounded
 * out-of-orderness generator. It forwards each batch unchanged (it is a pass-through over the data)
 * and side-channels {@link Watermark} events, so a columnar source can feed a columnar window
 * without transposing to {@link org.apache.flink.table.data.RowData} just to assign watermarks.
 *
 * <p>It mirrors the host exactly for the {@code rt - INTERVAL} (bounded out-of-orderness) form: the
 * candidate watermark is {@code max(rowtime - interval)}, the running max never falls below 0, and
 * the watermark is emitted eagerly when it jumps by more than the auto-watermark interval and
 * otherwise on a periodic processing-time timer at that interval.
 *
 * <p>To match the host's per-row late-data dropping byte for byte, the watermark must advance at
 * the same fine granularity within a batch — a row is dropped downstream only if a watermark
 * closing its window was emitted before it. The host advances per row; we replicate that by slicing
 * a batch at each point its running watermark jumps past the interval and emitting {@code
 * (sub-batch, watermark)} in order, so the fine-grained watermarks propagate through the shuffle
 * and the downstream drops exactly as the host would (the drop can't be done downstream: at
 * parallelism > 1 a window's effective watermark is the min across its input channels, which a
 * post-shuffle operator cannot reconstruct). Even sorted rows retain these boundaries because a
 * downstream {@code CURRENT_WATERMARK} can observe each eager emission. A batch without an internal
 * emission boundary is forwarded whole. Idleness is not modelled for this assigner.
 */
public class NativeColumnarWatermarkAssignerOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final int rowtimeColumn;
  private final WatermarkExpression expression;

  private transient WatermarkExpression.Evaluator evaluator;

  private transient long currentWatermark;
  private transient long lastWatermark;
  private transient long watermarkInterval;
  private transient long lastWatermarkPeriodicEmitTime;

  public NativeColumnarWatermarkAssignerOperator(int rowtimeColumn, long delayMillis) {
    this(rowtimeColumn, WatermarkExpression.subtractMillis(rowtimeColumn, delayMillis));
  }

  public NativeColumnarWatermarkAssignerOperator(
      int rowtimeColumn, WatermarkExpression expression) {
    this.rowtimeColumn = rowtimeColumn;
    this.expression = expression;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    evaluator = expression.open();
    // Watermark and timestamp start from 0, as the host's assigner does.
    currentWatermark = 0;
    lastWatermark = 0;
    watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
    if (watermarkInterval > 0) {
      long now = getProcessingTimeService().getCurrentProcessingTime();
      lastWatermarkPeriodicEmitTime = now;
      getProcessingTimeService().registerTimer(now + watermarkInterval, this);
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (evaluator != null) {
        evaluator.close();
      }
    } finally {
      super.close();
    }
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    // Taking the root disarms the batch's abandoned-record backstop, so the forward-as-is paths
    // below re-wrap it in a fresh ArrowBatch rather than forwarding the disarmed element.
    VectorSchemaRoot root = element.getValue().root();
    int rows = root.getRowCount();
    if (rows == 0) {
      ColumnarRecordMetrics.forward(
          output, getMetricGroup(), element.replace(new ArrowBatch(root)), rows);
      return;
    }
    if (root.getVector(rowtimeColumn).getNullCount() > 0) {
      root.close();
      throw new RuntimeException(
          "RowTime field should not be null, please convert it to a non-null long value.");
    }
    boolean forwarded = false;
    try (WatermarkExpression.Values candidates = evaluator.evaluate(root)) {
      int sliceStart = 0;
      for (int i = 0; i < rows; i++) {
        if (!candidates.isNull(i)) {
          currentWatermark = Math.max(currentWatermark, candidates.getMillis(i));
        }
        boolean emitWatermark = currentWatermark - lastWatermark > watermarkInterval;
        if (emitWatermark || i == rows - 1) {
          int count = i - sliceStart + 1;
          if (count == rows) {
            forwarded = true;
            ColumnarRecordMetrics.forward(
                output, getMetricGroup(), element.replace(new ArrowBatch(root)), rows);
          } else {
            ColumnarRecordMetrics.emit(
                output, getMetricGroup(), new ArrowBatch(root.slice(sliceStart, count)));
          }
          sliceStart = i + 1;
          if (emitWatermark) {
            advanceWatermark();
          }
        }
      }
    } finally {
      // Slices and native exports retain their own references. Whole-batch forwarding moves ours.
      if (!forwarded) {
        root.close();
      }
    }
  }

  private void advanceWatermark() {
    if (currentWatermark > lastWatermark) {
      lastWatermark = currentWatermark;
      output.emitWatermark(new Watermark(currentWatermark));
    }
  }

  @Override
  public void onProcessingTime(long timestamp) {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    if (watermarkInterval > 0 && lastWatermarkPeriodicEmitTime + watermarkInterval <= now) {
      lastWatermarkPeriodicEmitTime = now;
      advanceWatermark();
    }
    getProcessingTimeService().registerTimer(now + watermarkInterval, this);
  }

  /**
   * Upstream watermarks are ignored — this operator is the watermark source — except the
   * end-of-input MAX_WATERMARK, which is forwarded to flush all downstream windows.
   */
  @Override
  public void processWatermark(Watermark mark) {
    if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
      currentWatermark = Long.MAX_VALUE;
      output.emitWatermark(mark);
    }
  }

  @Override
  public void finish() throws Exception {
    // All records processed: emit the final (MAX) watermark, as the host's assigner does.
    processWatermark(Watermark.MAX_WATERMARK);
    super.finish();
  }
}
