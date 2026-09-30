package tech.streamfusion.paimon;

import java.util.Map;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.core.io.InputStatus;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.ReaderConsumeProgressEvent;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.WatermarkExpression;

/** Mirrors released Paimon's split requests and consumer-progress events. */
final class NativePaimonSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        NativeSourceRecord, ArrowBatch, FileStoreSourceSplit, PaimonSourceSplitState> {
  private final IOManager io;
  private long lastSnapshot = Long.MIN_VALUE;
  private final boolean watermarked;
  private PaimonSplitWatermarks watermarkOutput;

  NativePaimonSourceReader(
      FileStoreTable table,
      ReadBuilder read,
      SourceReaderContext context,
      int batchRows,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression) {
    this(
        table,
        read,
        context,
        batchRows,
        rowtimeIndex,
        watermarkExpression,
        IOManager.create(
            ConfigurationUtils.splitPaths(context.getConfiguration().get(CoreOptions.TMP_DIRS))),
        new FileStoreSourceReaderMetrics(context.metricGroup()));
  }

  private NativePaimonSourceReader(
      FileStoreTable table,
      ReadBuilder read,
      SourceReaderContext context,
      int batchRows,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression,
      IOManager io,
      FileStoreSourceReaderMetrics metrics) {
    super(
        () ->
            new NativePaimonSplitReader(
                    table,
                    read,
                    PaimonVersion.withMetrics(
                        read.newRead().withIOManager(io),
                        new FlinkMetricRegistry(context.metricGroup())),
                    batchRows,
                    rowtimeIndex,
                    watermarkExpression)
                .withMetrics(metrics),
        (record, output, state) -> {
          context
              .metricGroup()
              .getIOMetricGroup()
              .getNumRecordsInCounter()
              .inc(record.nextOffset() - state.emitted - 1);
          record.emit(output, position -> state.emitted = position);
        },
        context.getConfiguration(),
        context);
    this.io = io;
    this.watermarked = rowtimeIndex >= 0;
  }

  @Override
  public InputStatus pollNext(ReaderOutput<ArrowBatch> output) throws Exception {
    if (!watermarked) {
      return super.pollNext(output);
    }
    if (watermarkOutput == null) {
      watermarkOutput = new PaimonSplitWatermarks(output);
    }
    return super.pollNext(watermarkOutput);
  }

  @Override
  public void start() {
    if (getNumberOfCurrentlyAssignedSplits() == 0) {
      context.sendSplitRequest();
    }
  }

  @Override
  protected void onSplitFinished(Map<String, PaimonSourceSplitState> finished) {
    if (getNumberOfCurrentlyAssignedSplits() == 0) {
      context.sendSplitRequest();
    }
    long latest =
        finished.values().stream()
            .filter(s -> s.split.split() instanceof DataSplit)
            .mapToLong(s -> ((DataSplit) s.split.split()).snapshotId())
            .max()
            .orElse(Long.MIN_VALUE);
    if (latest > lastSnapshot) {
      lastSnapshot = latest;
      context.sendSourceEventToCoordinator(new ReaderConsumeProgressEvent(latest));
    }
  }

  @Override
  protected PaimonSourceSplitState initializedState(FileStoreSourceSplit split) {
    return new PaimonSourceSplitState(split);
  }

  @Override
  protected FileStoreSourceSplit toSplitType(String id, PaimonSourceSplitState state) {
    return state.checkpoint();
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } finally {
      io.close();
    }
  }
}
