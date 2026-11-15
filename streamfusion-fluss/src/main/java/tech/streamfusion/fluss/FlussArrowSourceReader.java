package tech.streamfusion.fluss;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.*;

/** Checkpoint positions advance on the task thread only after downstream collection succeeds. */
final class FlussArrowSourceReader implements SourceReader<ArrowBatch, SourceSplitBase> {
  private final SourceReaderContext context;
  private final FlussArrowClient client;
  private final int rowtimeIndex;
  private final WatermarkExpression.Evaluator watermark;
  private final ExecutorService fetcher;
  private final Map<String, LogSplit> splits = new LinkedHashMap<>();
  private final Deque<String> schedule = new ArrayDeque<>();
  private final Deque<FlussArrowClient.Fetched> records = new ArrayDeque<>();
  private CompletableFuture<List<FlussArrowClient.Fetched>> pending;
  private CompletableFuture<Void> assigned = new CompletableFuture<>();
  private String fetchingSplit;
  private boolean noMoreSplits;
  private final Map<String, LogSplit> retiringSplits = new LinkedHashMap<>();
  private final Set<TableBucket> removedBuckets = new HashSet<>();
  private final List<String> removedOutputs = new ArrayList<>();
  private volatile boolean closed;

  FlussArrowSourceReader(
      SourceReaderContext context,
      Configuration config,
      TablePath path,
      RowType outputType,
      int rowtimeIndex,
      WatermarkExpression watermark)
      throws Exception {
    this(context, config, path, outputType, rowtimeIndex, watermark, null);
  }

  FlussArrowSourceReader(
      SourceReaderContext context,
      Configuration config,
      TablePath path,
      RowType outputType,
      int rowtimeIndex,
      WatermarkExpression watermark,
      Predicate filter)
      throws Exception {
    TaskOffHeapMemory.initialize(context.getConfiguration());
    TaskOffHeapMemory.registerMetrics(context.metricGroup());
    this.context = context;
    this.rowtimeIndex = rowtimeIndex;
    this.watermark = watermark == null ? null : watermark.open();
    client =
        new FlussArrowClient(
            config,
            path,
            ArrowConversion.toArrowSchema(outputType),
            NativeAllocator.SHARED,
            true,
            filter);
    fetcher =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "streamfusion-fluss-fetcher");
              thread.setDaemon(true);
              return thread;
            });
  }

  @Override
  public void start() {
    context.sendSplitRequest();
  }

  @Override
  public InputStatus pollNext(ReaderOutput<ArrowBatch> output) throws Exception {
    for (String id : removedOutputs) output.releaseOutputForSplit(id);
    removedOutputs.clear();
    if (pending != null && pending.isDone()) {
      if (!splits.containsKey(fetchingSplit)) {
        try {
          pending.get().forEach(FlussArrowClient.Fetched::close);
        } catch (java.util.concurrent.ExecutionException removedFetch) {
          // The removed partition may already be unavailable at the broker.
        }
        fetchingSplit = null;
      } else {
        records.addAll(pending.get());
      }
      pending = null;
    }
    if (pending == null && !removedBuckets.isEmpty()) {
      context.sendSourceEventToCoordinator(
          new PartitionBucketsUnsubscribedEvent(Set.copyOf(removedBuckets)));
      removedBuckets.clear();
      retiringSplits.clear();
    }
    if (!records.isEmpty()) {
      var record = records.removeFirst();
      LogSplit split = splits.get(fetchingSplit);
      var nativeRecord =
          record.root() == null
              ? new NativeSourceRecord(null, record.nextOffset(), Long.MIN_VALUE)
              : NativeSourceRecord.fromRoot(
                  record.root(), record.nextOffset(), rowtimeIndex, watermark);
      try {
        nativeRecord.emit(
            output.createOutputForSplit(fetchingSplit),
            next ->
                splits.put(
                    fetchingSplit,
                    new LogSplit(
                        split.getTableBucket(),
                        split.getPartitionName(),
                        next,
                        split.getStoppingOffset().orElse(LogSplit.NO_STOPPING_OFFSET))));
      } catch (Throwable failure) {
        nativeRecord.discardUnclaimed();
        throw failure;
      }
      if (!records.isEmpty()) return InputStatus.MORE_AVAILABLE;
    }
    if (pending == null && records.isEmpty()) {
      if (fetchingSplit != null) {
        LogSplit split = splits.get(fetchingSplit);
        if (split.getStoppingOffset().isPresent()
            && split.getStartingOffset() >= split.getStoppingOffset().get()) {
          splits.remove(fetchingSplit);
          schedule.remove(fetchingSplit);
          output.releaseOutputForSplit(fetchingSplit);
        }
        fetchingSplit = null;
      }
      submitFetch();
    }
    if (noMoreSplits && splits.isEmpty()) return InputStatus.END_OF_INPUT;
    return pending != null && pending.isDone()
        ? InputStatus.MORE_AVAILABLE
        : InputStatus.NOTHING_AVAILABLE;
  }

  private void submitFetch() {
    if (pending != null || schedule.isEmpty() || closed) return;
    fetchingSplit = schedule.removeFirst();
    schedule.addLast(fetchingSplit);
    LogSplit split = splits.get(fetchingSplit);
    pending =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                List<FlussArrowClient.Fetched> result = client.fetch(split);
                if (closed) {
                  result.forEach(FlussArrowClient.Fetched::close);
                  return List.of();
                }
                return result;
              } catch (org.apache.fluss.exception.PartitionNotExistException removedPartition) {
                if (split.getTableBucket().getPartitionId() != null) return List.of();
                throw removedPartition;
              } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
              }
            },
            fetcher);
  }

  @Override
  public List<SourceSplitBase> snapshotState(long checkpointId) {
    var checkpointSplits = new ArrayList<SourceSplitBase>(splits.values());
    checkpointSplits.addAll(retiringSplits.values());
    return checkpointSplits;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    if (!records.isEmpty() || !removedOutputs.isEmpty() || noMoreSplits && splits.isEmpty())
      return CompletableFuture.completedFuture(null);
    submitFetch();
    return pending == null ? assigned : pending.handle((ignored, failure) -> null);
  }

  @Override
  public void addSplits(List<SourceSplitBase> additions) {
    for (SourceSplitBase base : additions) {
      if (!base.isLogSplit())
        throw new IllegalArgumentException("Fluss Arrow source admits log splits only");
      LogSplit split = base.asLogSplit();
      if (splits.putIfAbsent(split.splitId(), split) != null)
        throw new IllegalArgumentException("Duplicate Fluss split");
      schedule.addLast(split.splitId());
    }
    assigned.complete(null);
    assigned = new CompletableFuture<>();
    submitFetch();
  }

  @Override
  public void notifyNoMoreSplits() {
    noMoreSplits = true;
    assigned.complete(null);
  }

  @Override
  public void handleSourceEvents(SourceEvent event) {
    if (!(event instanceof PartitionsRemovedEvent)) return;
    PartitionsRemovedEvent removed = ((PartitionsRemovedEvent) event);

    var iterator = splits.entrySet().iterator();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      TableBucket bucket = entry.getValue().getTableBucket();
      if (!removed.getRemovedPartitions().containsKey(bucket.getPartitionId())) continue;
      retiringSplits.put(entry.getKey(), entry.getValue());
      removedBuckets.add(bucket);
      removedOutputs.add(entry.getKey());
      schedule.remove(entry.getKey());
      iterator.remove();
    }
    if (fetchingSplit != null && !splits.containsKey(fetchingSplit)) {
      records.forEach(FlussArrowClient.Fetched::close);
      records.clear();
      if (pending == null) fetchingSplit = null;
    }
    if (pending == null) {
      context.sendSourceEventToCoordinator(
          new PartitionBucketsUnsubscribedEvent(Set.copyOf(removedBuckets)));
      removedBuckets.clear();
      retiringSplits.clear();
    }
    assigned.complete(null);
  }

  @Override
  public void close() throws Exception {
    closed = true;
    records.forEach(FlussArrowClient.Fetched::close);
    records.clear();
    if (pending != null)
      pending.thenAccept(result -> result.forEach(FlussArrowClient.Fetched::close));
    fetcher.shutdownNow();
    try {
      client.close();
    } finally {
      if (watermark != null) watermark.close();
    }
  }
}
