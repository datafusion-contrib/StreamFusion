package tech.streamfusion.fluss;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TablePath;
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
  private volatile boolean closed;

  FlussArrowSourceReader(
      SourceReaderContext context,
      Configuration config,
      TablePath path,
      RowType outputType,
      int rowtimeIndex,
      WatermarkExpression watermark)
      throws Exception {
    TaskOffHeapMemory.initialize(context.getConfiguration());
    TaskOffHeapMemory.registerMetrics(context.metricGroup());
    this.context = context;
    this.rowtimeIndex = rowtimeIndex;
    this.watermark = watermark == null ? null : watermark.open();
    client =
        new FlussArrowClient(
            config, path, ArrowConversion.toArrowSchema(outputType), NativeAllocator.SHARED);
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
    if (pending != null && pending.isDone()) {
      List<FlussArrowClient.Fetched> fetched = pending.get();
      records.addAll(fetched);
      pending = null;
    }
    if (!records.isEmpty()) {
      var record = records.removeFirst();
      LogSplit split = splits.get(fetchingSplit);
      try {
        var nativeRecord =
            record.root() == null
                ? new NativeSourceRecord(null, record.nextOffset(), Long.MIN_VALUE)
                : NativeSourceRecord.fromRoot(
                    record.root(), record.nextOffset(), rowtimeIndex, watermark);
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
        record.close();
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
              } catch (Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
              }
            },
            fetcher);
  }

  @Override
  public List<SourceSplitBase> snapshotState(long checkpointId) {
    return new ArrayList<>(splits.values());
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    if (!records.isEmpty() || noMoreSplits && splits.isEmpty())
      return CompletableFuture.completedFuture(null);
    submitFetch();
    return pending == null ? assigned : pending.thenApply(ignored -> null);
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
