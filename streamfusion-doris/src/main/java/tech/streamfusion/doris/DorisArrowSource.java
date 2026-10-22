package tech.streamfusion.doris;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.enumerator.DorisSourceCheckpoint;
import org.apache.doris.flink.source.reader.DorisOffsetPublisher;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Reuses Doris enumeration, split serialization and whole-split recovery for every scan mode. */
public final class DorisArrowSource
    implements Source<ArrowBatch, DorisSourceSplit, DorisSourceCheckpoint>, DorisSourceRuntime {
  private static final long serialVersionUID = 1L;
  private final DorisSource<?> delegate;
  private final DorisOptions options;
  private final DorisReadOptions reads;
  private final RowType type;

  public DorisArrowSource(
      DorisSource<?> delegate, DorisOptions options, DorisReadOptions reads, RowType type) {
    this.delegate = delegate;
    this.options = options;
    this.reads = reads;
    this.type = type;
  }

  @Override
  public org.apache.flink.api.dag.Transformation<ArrowBatch> addTo(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment) {
    return environment
        .fromSource(
            this,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
            "native-doris-source",
            tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE)
        .getTransformation();
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SplitEnumerator<DorisSourceSplit, DorisSourceCheckpoint> createEnumerator(
      SplitEnumeratorContext<DorisSourceSplit> context) throws Exception {
    return delegate.createEnumerator(context);
  }

  @Override
  public SplitEnumerator<DorisSourceSplit, DorisSourceCheckpoint> restoreEnumerator(
      SplitEnumeratorContext<DorisSourceSplit> context, DorisSourceCheckpoint checkpoint)
      throws Exception {
    return delegate.restoreEnumerator(context, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<DorisSourceSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<DorisSourceCheckpoint> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }

  @Override
  public SourceReader<ArrowBatch, DorisSourceSplit> createReader(SourceReaderContext context) {
    TaskOffHeapMemory.initialize(context.getConfiguration());
    TaskOffHeapMemory.registerMetrics(context.metricGroup());
    return new Reader(context);
  }

  final class Reader
      extends SingleThreadMultiplexSourceReaderBase<
          ArrowBatch, ArrowBatch, DorisSourceSplit, DorisSourceSplit> {
    private final SortedMap<Long, String> offsets =
        Collections.synchronizedSortedMap(new TreeMap<>());
    private String lastFinishedOffset;

    Reader(SourceReaderContext context) {
      super(
          new Fetchers(context.getConfiguration()),
          (batch, output, split) -> {
            var record = new NativeSourceRecord(batch, 0, Long.MIN_VALUE);
            try {
              record.emit(output, ignored -> {});
            } catch (Throwable failure) {
              record.discardUnclaimed();
              throw failure;
            }
          },
          context.getConfiguration(),
          context);
    }

    @Override
    public void start() {
      if (getNumberOfCurrentlyAssignedSplits() == 0) context.sendSplitRequest();
    }

    @Override
    protected void onSplitFinished(Map<String, DorisSourceSplit> finished) {
      for (var split : finished.values())
        if (split instanceof DorisStreamSplit)
          lastFinishedOffset = ((DorisStreamSplit) split).getEndTimestamp();
      if (getNumberOfCurrentlyAssignedSplits() == 0) context.sendSplitRequest();
    }

    @Override
    public List<DorisSourceSplit> snapshotState(long checkpointId) {
      var splits = super.snapshotState(checkpointId);
      if (reads.getBinlogOffsetTable() != null
          && context.getIndexOfSubtask() == 0
          && lastFinishedOffset != null) offsets.put(checkpointId, lastFinishedOffset);
      return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      String offset = offsets.get(checkpointId);
      if (offset == null) return;
      ((Fetchers) splitFetcherManager)
          .publish(
              offset,
              error -> {
                if (error != null)
                  org.slf4j.LoggerFactory.getLogger(DorisArrowSource.class)
                      .warn(
                          "Failed to publish Doris source offset {} for checkpoint {}",
                          offset,
                          checkpointId,
                          error);
                synchronized (offsets) {
                  while (!offsets.isEmpty() && offsets.firstKey() <= checkpointId)
                    offsets.remove(offsets.firstKey());
                }
              });
    }

    @Override
    protected DorisSourceSplit initializedState(DorisSourceSplit split) {
      return split;
    }

    @Override
    protected DorisSourceSplit toSplitType(String id, DorisSourceSplit split) {
      return split;
    }
  }

  private final class Fetchers extends SingleThreadFetcherManager<ArrowBatch, DorisSourceSplit> {
    Fetchers(org.apache.flink.configuration.Configuration configuration) {
      super(() -> new Splits(), configuration);
    }

    void publish(String offset, Consumer<Exception> callback) {
      var fetcher = getRunningFetcher();
      boolean start = fetcher == null;
      if (start) fetcher = createSplitFetcher();
      Splits reader = (Splits) fetcher.getSplitReader();
      fetcher.enqueueTask(
          new SplitFetcherTask() {
            @Override
            public boolean run() {
              reader.publisher.publish(offset, callback);
              return true;
            }

            @Override
            public void wakeUp() {}
          });
      if (start) startFetcher(fetcher);
    }
  }

  private final class Splits implements SplitReader<ArrowBatch, DorisSourceSplit> {
    private final DorisOffsetPublisher publisher =
        reads.getBinlogOffsetTable() == null
            ? null
            : new DorisOffsetPublisher(
                new SimpleJdbcConnectionProvider(options),
                reads.getBinlogOffsetTable(),
                reads.getBinlogConsumerId());
    private final Queue<DorisSourceSplit> pending = new ArrayDeque<>();
    private DorisSourceSplit split;
    private DorisBatchReader reader;

    @Override
    public RecordsWithSplitIds<ArrowBatch> fetch() throws IOException {
      if (reader == null) {
        split = pending.remove();
        reader =
            DorisBatchReader.create(split, options, reads, ArrowConversion.toArrowSchema(type));
      }
      var result = new RecordsBySplits.Builder<ArrowBatch>();
      var root = reader.readBatch();
      if (root == null) {
        result.addFinishedSplit(split.splitId());
        reader.close();
        reader = null;
      } else return new BatchRecords(split.splitId(), new ArrowBatch(root));
      return result.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DorisSourceSplit> change) {
      pending.addAll(change.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws IOException {
      try {
        if (reader != null) reader.close();
      } finally {
        if (publisher != null) publisher.close();
      }
    }
  }

  /**
   * Releases batches drained from the fetch queue on cancellation, before the emitter takes them.
   */
  static final class BatchRecords extends RecordsBySplits<ArrowBatch> {
    private ArrowBatch pending;

    BatchRecords(String split, ArrowBatch batch) {
      super(Map.of(split, List.of(batch)), Set.of());
      pending = batch;
    }

    @Override
    public ArrowBatch nextRecordFromSplit() {
      ArrowBatch batch = super.nextRecordFromSplit();
      if (batch != null) pending = null;
      return batch;
    }

    @Override
    public void recycle() {
      if (pending != null) {
        new NativeSourceRecord(pending, 0, Long.MIN_VALUE).discardUnclaimed();
        pending = null;
      }
    }
  }
}
