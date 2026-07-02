package io.github.jordepic.streamfusion.operator;

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar event-time watermark assigner: the Arrow-batch analog of the host's {@link
 * org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator} with a bounded
 * out-of-orderness generator. It forwards each batch unchanged (it is a pass-through over the data)
 * and side-channels {@link Watermark} events, so a columnar source can feed a columnar window
 * without transposing to {@link org.apache.flink.table.data.RowData} just to assign watermarks.
 *
 * <p>It mirrors the host exactly for the {@code rt - INTERVAL} (bounded out-of-orderness) form: the
 * candidate watermark is {@code max(rowtime) - delay}, the running max never falls below 0, and the
 * watermark is emitted eagerly when it jumps by more than the auto-watermark interval and otherwise
 * on a periodic processing-time timer at that interval.
 *
 * <p>To match the host's per-row late-data dropping byte for byte, the watermark must advance at the
 * same fine granularity within a batch — a row is dropped downstream only if a watermark closing its
 * window was emitted before it. The host advances per row; we replicate that by slicing a batch at
 * each point its running watermark jumps past the interval and emitting {@code (sub-batch,
 * watermark)} in order, so the fine-grained watermarks propagate through the shuffle and the
 * downstream drops exactly as the host would (the drop can't be done downstream: at parallelism > 1
 * a window's effective watermark is the min across its input channels, which a post-shuffle operator
 * cannot reconstruct). A monotonic-rowtime batch can have no within-batch late row — a later row's
 * window can never be closed by an earlier, smaller rowtime — so it takes a fast path that forwards
 * the whole batch with a single watermark (no slicing, the common in-order case). Idleness is not
 * modelled — the filesystem sources this accelerates are never idle.
 */
public class NativeColumnarWatermarkAssignerOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final int rowtimeColumn;
  private final long delayMillis;

  private transient long currentWatermark;
  private transient long lastWatermark;
  private transient long watermarkInterval;
  private transient long lastWatermarkPeriodicEmitTime;

  public NativeColumnarWatermarkAssignerOperator(int rowtimeColumn, long delayMillis) {
    this.rowtimeColumn = rowtimeColumn;
    this.delayMillis = delayMillis;
  }

  @Override
  public void open() throws Exception {
    super.open();
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
  public void processElement(StreamRecord<ArrowBatch> element) {
    // Taking the root disarms the batch's abandoned-record backstop, so the forward-as-is paths
    // below re-wrap it in a fresh ArrowBatch rather than forwarding the disarmed element.
    VectorSchemaRoot root = element.getValue().root();
    int rows = root.getRowCount();
    if (rows == 0) {
      output.collect(element.replace(new ArrowBatch(root)));
      return;
    }
    TimeStampVector rt = (TimeStampVector) root.getVector(rowtimeColumn);
    ArrowType.Timestamp type = (ArrowType.Timestamp) rt.getField().getType();
    if (isMonotonic(rt, type, rows)) {
      // No row can be late within a monotonic batch, so the host would drop nothing either: forward
      // the whole batch (the max is the last row) with a single eager watermark.
      currentWatermark = Math.max(currentWatermark, toMillis(rt.get(rows - 1), type) - delayMillis);
      output.collect(element.replace(new ArrowBatch(root)));
      if (currentWatermark - lastWatermark > watermarkInterval) {
        advanceWatermark();
      }
      return;
    }
    // Out-of-order: replicate the host's per-row eager emission, slicing the batch at each watermark
    // jump so a window-closing watermark precedes any row it makes late (just as the host forwards a
    // row before the watermark it triggers).
    int sliceStart = 0;
    for (int i = 0; i < rows; i++) {
      currentWatermark = Math.max(currentWatermark, toMillis(rt.get(i), type) - delayMillis);
      if (currentWatermark - lastWatermark > watermarkInterval) {
        output.collect(new StreamRecord<>(new ArrowBatch(root.slice(sliceStart, i - sliceStart + 1))));
        sliceStart = i + 1;
        advanceWatermark();
      }
    }
    if (sliceStart < rows) {
      output.collect(new StreamRecord<>(new ArrowBatch(root.slice(sliceStart, rows - sliceStart))));
    }
    // The slices retain their own references to the shared buffers; release the original batch.
    root.close();
  }

  /** Whether the rowtime column is non-decreasing — then no row is late relative to an earlier one. */
  private static boolean isMonotonic(TimeStampVector rt, ArrowType.Timestamp type, int rows) {
    long prev = Long.MIN_VALUE;
    for (int i = 0; i < rows; i++) {
      long millis = toMillis(rt.get(i), type);
      if (millis < prev) {
        return false;
      }
      prev = millis;
    }
    return true;
  }

  /** Reduces a timestamp in the column's own unit to epoch millis (how watermarks are expressed). */
  private static long toMillis(long raw, ArrowType.Timestamp type) {
    switch (type.getUnit()) {
      case SECOND:
        return raw * 1_000L;
      case MILLISECOND:
        return raw;
      case MICROSECOND:
        return raw / 1_000L;
      case NANOSECOND:
      default:
        return raw / 1_000_000L;
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
