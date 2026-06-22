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
 * on a periodic processing-time timer at that interval. Reading the rowtime once per batch (the max
 * of the column) is equivalent to the host's per-row max. Idleness is not modelled — the filesystem
 * sources this accelerates are never idle.
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
    VectorSchemaRoot root = element.getValue().root();
    int rows = root.getRowCount();
    if (rows > 0) {
      TimeStampVector rt = (TimeStampVector) root.getVector(rowtimeColumn);
      ArrowType.Timestamp type = (ArrowType.Timestamp) rt.getField().getType();
      long maxMillis = Long.MIN_VALUE;
      for (int i = 0; i < rows; i++) {
        long millis = toMillis(rt.get(i), type);
        if (millis > maxMillis) {
          maxMillis = millis;
        }
      }
      currentWatermark = Math.max(currentWatermark, maxMillis - delayMillis);
    }
    // Forward the batch unchanged — the downstream operator owns and closes it.
    output.collect(element);
    // Eagerly emit when the watermark jumps by more than the interval, so a high-throughput run
    // does not wait on the periodic timer (matches the host).
    if (currentWatermark - lastWatermark > watermarkInterval) {
      advanceWatermark();
    }
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
