package tech.streamfusion.iceberg;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** The released source retains discovery/state; Arrow allocations use the common task budget. */
final class IcebergArrowSource
    implements Source<ArrowBatch, IcebergSourceSplit, IcebergEnumeratorState> {
  private final IcebergSource<ArrowBatch> delegate;
  private final IcebergArrowReaderFunction function;

  IcebergArrowSource(IcebergSource<ArrowBatch> delegate, IcebergArrowReaderFunction function) {
    this.function = function;
    this.delegate = delegate;
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<ArrowBatch, IcebergSourceSplit> createReader(SourceReaderContext context) {
    TaskOffHeapMemory.initialize(context.getConfiguration());
    TaskOffHeapMemory.registerMetrics(context.metricGroup());
    function.registerMetrics(context.metricGroup());
    return new IcebergArrowSourceReader(
        delegate.createReader(context),
        context.metricGroup().getIOMetricGroup().getNumRecordsInCounter(),
        context.metricGroup().getIOMetricGroup().getNumRecordsOutCounter(),
        function);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> context) {
    return delegate.createEnumerator(context);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> context, IcebergEnumeratorState state) {
    return delegate.restoreEnumerator(context, state);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<IcebergEnumeratorState> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }
}
