package tech.streamfusion.operator;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Fan-out point for a shared native sub-plan: declares on each passing batch how many chained
 * consumers will take it, so {@link ArrowBatch#root()} hands every consumer its own retained view
 * instead of the single-owner root. The record itself is forwarded untouched — Flink's chained
 * broadcast delivers the same object to every consumer ({@link ArrowBatchSerializer#copy} is
 * identity), and watermarks and barriers follow the normal broadcast path. This is the plan-time
 * consumer count RisingWave's {@code StreamShare} carries; the sharing itself is Arrow buffer
 * reference counting, the analog of Arroyo's {@code Arc<RecordBatch>} clone.
 */
public class NativeColumnarShareOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private static final long serialVersionUID = -3496659690003591615L;

  private final int consumers;
  private transient int deployedConsumers;

  public NativeColumnarShareOperator(int consumers) {
    this.consumers = consumers;
  }

  @Override
  public void open() throws Exception {
    super.open();
    // A reused network result can serve many logical consumers but takes the root once when
    // serializing. Only independent deployed outputs spend an ownership share.
    var loader = getUserCodeClassloader();
    deployedConsumers =
        getOperatorConfig().getChainedOutputs(loader).size()
            + getOperatorConfig().getOperatorNonChainedOutputs(loader).size();
    if (deployedConsumers < 1)
      throw new IllegalStateException("Native share has no deployed outputs");
    getMetricGroup().gauge("sharePlannedConsumers", () -> consumers);
    getMetricGroup().gauge("shareDeployedConsumers", () -> deployedConsumers);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    element.getValue().shareAcross(deployedConsumers);
    ColumnarRecordMetrics.forward(output, getMetricGroup(), element, element.getValue().rowCount());
  }
}
