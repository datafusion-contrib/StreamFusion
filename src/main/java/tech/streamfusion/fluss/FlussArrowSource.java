package tech.streamfusion.fluss;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.FlinkSource;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
import org.apache.fluss.metadata.TablePath;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.WatermarkExpression;

/** Keeps Fluss's enumerator and split serializers; task readers emit columnar log batches. */
public final class FlussArrowSource
    implements Source<ArrowBatch, SourceSplitBase, SourceEnumeratorState> {
  private static final long serialVersionUID = 1L;
  private final FlinkSource<?> delegate;
  private final Configuration config;
  private final TablePath path;
  private final RowType outputType;
  private final int rowtimeIndex;
  private final WatermarkExpression watermark;

  public FlussArrowSource(
      FlinkSource<?> delegate,
      Configuration config,
      TablePath path,
      RowType outputType,
      int rowtimeIndex,
      WatermarkExpression watermark) {
    this.delegate = delegate;
    this.config = config;
    this.path = path;
    this.outputType = outputType;
    this.rowtimeIndex = rowtimeIndex;
    this.watermark = watermark;
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<ArrowBatch, SourceSplitBase> createReader(SourceReaderContext context)
      throws Exception {
    return new FlussArrowSourceReader(context, config, path, outputType, rowtimeIndex, watermark);
  }

  @Override
  public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> createEnumerator(
      SplitEnumeratorContext<SourceSplitBase> context) throws Exception {
    return delegate.createEnumerator(context);
  }

  @Override
  public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<SourceSplitBase> context, SourceEnumeratorState checkpoint)
      throws Exception {
    return delegate.restoreEnumerator(context, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<SourceEnumeratorState> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }
}
