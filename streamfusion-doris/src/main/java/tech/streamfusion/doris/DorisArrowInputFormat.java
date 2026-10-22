package tech.streamfusion.doris;

import java.io.IOException;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.table.DorisRowDataInputFormat;
import org.apache.doris.flink.table.DorisTableInputSplit;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Uses the released legacy split assigner and scanner with Arrow records. */
public final class DorisArrowInputFormat extends RichInputFormat<ArrowBatch, DorisTableInputSplit>
    implements DorisSourceRuntime {
  private static final long serialVersionUID = 1L;
  private final DorisRowDataInputFormat delegate;
  private final DorisOptions options;
  private final DorisReadOptions reads;
  private final RowType type;
  private transient DorisBatchReader reader;
  private transient ArrowBatch pending;

  public DorisArrowInputFormat(DorisOptions options, DorisReadOptions reads, RowType type) {
    this.options = options;
    this.reads = reads;
    this.type = type;
    try {
      delegate =
          new DorisRowDataInputFormat(
              options,
              org.apache.doris.flink.rest.RestService.findPartitions(
                  options, reads, org.slf4j.LoggerFactory.getLogger(DorisArrowInputFormat.class)),
              reads,
              type);
    } catch (Exception failure) {
      throw new IllegalStateException("Failed to prepare Doris legacy Arrow splits", failure);
    }
  }

  @Override
  public Transformation<ArrowBatch> addTo(StreamExecutionEnvironment environment) {
    return environment
        .addSource(
            new GuardedSource(this), "native-doris-source", ArrowBatchTypeInformation.INSTANCE)
        .getTransformation();
  }

  @Override
  public void configure(Configuration configuration) {
    delegate.configure(configuration);
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cached) throws IOException {
    return delegate.getStatistics(cached);
  }

  @Override
  public DorisTableInputSplit[] createInputSplits(int minimum) throws IOException {
    return delegate.createInputSplits(minimum);
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(DorisTableInputSplit[] splits) {
    return delegate.getInputSplitAssigner(splits);
  }

  @Override
  public void open(DorisTableInputSplit split) throws IOException {
    var context =
        (org.apache.flink.streaming.api.operators.StreamingRuntimeContext) getRuntimeContext();
    TaskOffHeapMemory.initialize(context.getTaskManagerRuntimeInfo().getConfiguration());
    TaskOffHeapMemory.registerMetrics(context.getMetricGroup());
    try {
      var field = DorisTableInputSplit.class.getDeclaredField("partition");
      field.setAccessible(true);
      reader =
          DorisBatchReader.create(
              new DorisSnapshotSplit(
                  "legacy-" + split.getSplitNumber(), (PartitionDefinition) field.get(split)),
              options,
              reads,
              ArrowConversion.toArrowSchema(type));
    } catch (ReflectiveOperationException failure) {
      throw new IOException("Incompatible Doris legacy split", failure);
    }
  }

  @Override
  public boolean reachedEnd() throws IOException {
    if (pending == null) {
      var root = reader.readBatch();
      if (root == null) return true;
      pending = new ArrowBatch(root);
    }
    return false;
  }

  @Override
  public ArrowBatch nextRecord(ArrowBatch reuse) throws IOException {
    if (reachedEnd()) return null;
    var result = pending;
    pending = null;
    return result;
  }

  @Override
  public void close() throws IOException {
    try {
      if (pending != null)
        new tech.streamfusion.operator.NativeSourceRecord(pending, 0, Long.MIN_VALUE)
            .discardUnclaimed();
    } finally {
      pending = null;
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }
  }

  private static final class GuardedSource
      extends org.apache.flink.streaming.api.functions.source.legacy.InputFormatSourceFunction<
          ArrowBatch> {
    private static final long serialVersionUID = 1L;

    GuardedSource(DorisArrowInputFormat format) {
      super(format, ArrowBatchTypeInformation.INSTANCE);
    }

    @Override
    public void run(SourceContext<ArrowBatch> context) throws Exception {
      super.run(
          new SourceContext<ArrowBatch>() {
            private void emit(ArrowBatch batch, Long timestamp) {
              try {
                if (timestamp == null) context.collect(batch);
                else context.collectWithTimestamp(batch, timestamp);
              } catch (RuntimeException | Error failure) {
                try {
                  new tech.streamfusion.operator.NativeSourceRecord(batch, 0, Long.MIN_VALUE)
                      .discardUnclaimed();
                } catch (RuntimeException | Error cleanup) {
                  failure.addSuppressed(cleanup);
                }
                throw failure;
              }
            }

            @Override
            public void collect(ArrowBatch batch) {
              emit(batch, null);
            }

            @Override
            public void collectWithTimestamp(ArrowBatch batch, long timestamp) {
              emit(batch, timestamp);
            }

            @Override
            public void emitWatermark(
                org.apache.flink.streaming.api.watermark.Watermark watermark) {
              context.emitWatermark(watermark);
            }

            @Override
            public void markAsTemporarilyIdle() {
              context.markAsTemporarilyIdle();
            }

            @Override
            public Object getCheckpointLock() {
              return context.getCheckpointLock();
            }

            @Override
            public void close() {
              context.close();
            }
          });
    }
  }
}
