package io.github.jordepic.streamfusion.operator;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar analog of Flink's {@code ProcTimeMiniBatchAssignerOperator}: it forwards each Arrow batch
 * unchanged and, on a processing-time timer every {@code intervalMs} (and lazily when a batch crosses
 * an interval boundary), emits a {@link Watermark} that marks a mini-batch boundary. Downstream native
 * mini-batch operators — the local GROUP BY aggregate — flush their bundle on that marker, exactly as
 * Flink's {@code MapBundleOperator} does, so the whole columnar island shares one mini-batch cadence.
 * Carries no per-row work and no state; Arrow in and out keeps it inside the island.
 */
public class NativeColumnarMiniBatchAssignerOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>,
        ProcessingTimeService.ProcessingTimeCallback {

  private final long intervalMs;
  private transient long currentWatermark;

  public NativeColumnarMiniBatchAssignerOperator(long intervalMs) {
    this.intervalMs = intervalMs;
  }

  @Override
  public void open() throws Exception {
    super.open();
    currentWatermark = 0;
    long now = getProcessingTimeService().getCurrentProcessingTime();
    getProcessingTimeService().registerTimer(now + intervalMs, this);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    long currentBatch = now - now % intervalMs;
    if (currentBatch > currentWatermark) {
      currentWatermark = currentBatch;
      output.emitWatermark(new Watermark(currentBatch));
    }
    output.collect(element);
  }

  @Override
  public void onProcessingTime(long timestamp) {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    long currentBatch = now - now % intervalMs;
    if (currentBatch > currentWatermark) {
      currentWatermark = currentBatch;
      output.emitWatermark(new Watermark(currentBatch));
    }
    getProcessingTimeService().registerTimer(currentBatch + intervalMs, this);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // Flink's processing-time assigner owns the mini-batch marker stream, so upstream event-time
    // watermarks must not introduce extra bundle boundaries. MAX remains the end-of-input signal.
    if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
      currentWatermark = Long.MAX_VALUE;
      output.emitWatermark(mark);
    }
  }
}
