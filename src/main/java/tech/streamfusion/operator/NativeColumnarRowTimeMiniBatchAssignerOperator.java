package tech.streamfusion.operator;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar analog of Flink's {@code RowTimeMiniBatchAssginerOperator}: forwards each Arrow batch
 * unchanged and filters the upstream event-time watermarks down to the mini-batch cadence — only a
 * watermark that crosses an interval boundary is forwarded, so downstream native mini-batch
 * operators flush once per event-time interval instead of per upstream watermark. Unlike the
 * proc-time assigner it generates nothing itself; the filtered watermark sequence is a pure
 * function of the input watermarks, so the operator is deterministic.
 */
public class NativeColumnarRowTimeMiniBatchAssignerOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final long intervalMs;
  private transient long currentWatermark;
  private transient long nextWatermark;

  public NativeColumnarRowTimeMiniBatchAssignerOperator(long intervalMs) {
    this.intervalMs = intervalMs;
  }

  @Override
  public void open() throws Exception {
    super.open();
    currentWatermark = 0;
    nextWatermark = miniBatchStart(currentWatermark) + intervalMs - 1;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    output.collect(element);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // A MAX_VALUE watermark signals end of input; forward it so downstream progress never blocks.
    if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
      currentWatermark = Long.MAX_VALUE;
      output.emitWatermark(mark);
      return;
    }
    currentWatermark = Math.max(currentWatermark, mark.getTimestamp());
    if (currentWatermark >= nextWatermark) {
      advanceWatermark();
    }
  }

  private void advanceWatermark() {
    output.emitWatermark(new Watermark(currentWatermark));
    long end = miniBatchStart(currentWatermark) + intervalMs - 1;
    nextWatermark = end > currentWatermark ? end : end + intervalMs;
  }

  @Override
  public void finish() throws Exception {
    super.finish();
    advanceWatermark(); // emit the buffered watermark
  }

  private long miniBatchStart(long watermark) {
    return watermark - (watermark + intervalMs) % intervalMs;
  }
}
