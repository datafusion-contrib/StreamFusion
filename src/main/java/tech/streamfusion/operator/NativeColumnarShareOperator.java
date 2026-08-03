package tech.streamfusion.operator;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Fan-out point for a shared native sub-plan: declares on each passing batch how many chained
 * consumers will take it, so {@link ArrowBatch#root()} hands every consumer its own retained view
 * instead of the single-owner root. The record itself is forwarded untouched — Flink's chained
 * broadcast delivers the same object to every consumer ({@link ArrowBatchSerializer#copy} is
 * identity), and watermarks and barriers follow the normal broadcast path. This is the plan-time
 * consumer count RisingWave's {@code StreamShare} carries; the sharing itself is Arrow buffer
 * reference counting, the analog of Arroyo's {@code Arc<RecordBatch>} clone.
 */
public class NativeColumnarShareOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int consumers;

  public NativeColumnarShareOperator(int consumers) {
    this.consumers = consumers;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    element.getValue().shareAcross(consumers);
    output.collect(element);
  }
}
