package tech.streamfusion.operator;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.batch.BoundedExecution;

/**
 * Persistent bounded Arrow execution shared by operators with different plans and result schemas.
 */
public abstract class NativeBoundedBatchOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, BoundedOneInput {
  private transient BoundedExecution execution;

  protected abstract long createTask(long schemaAddress);

  protected abstract String metricPrefix();

  protected Schema outputSchema(Schema input) {
    return input;
  }

  protected Schema emptyInputSchema() {
    return null;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    execution =
        new BoundedExecution(
            this::createTask,
            this::outputSchema,
            this::emptyInputSchema,
            batch -> ColumnarRecordMetrics.emit(output, getMetricGroup(), batch),
            getMetricGroup());
    getMetricGroup().gauge(metricPrefix() + "SpillCount", () -> execution.statistics()[0]);
    getMetricGroup().gauge(metricPrefix() + "SpilledBytes", () -> execution.statistics()[1]);
    getMetricGroup().gauge(metricPrefix() + "ReservedBytes", () -> execution.statistics()[2]);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    execution.accept(element.getValue().root());
  }

  @Override
  public void endInput() {
    execution.endInput();
  }

  @Override
  public void close() throws Exception {
    try {
      if (execution != null) execution.close();
    } finally {
      super.close();
    }
  }
}
