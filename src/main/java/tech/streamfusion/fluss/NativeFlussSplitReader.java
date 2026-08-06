package tech.streamfusion.fluss;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.BoundedSplitTracker;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeSourceRecord;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;

/**
 * Native Fluss split reader for one Flink subtask. Fluss' enumerator assigns concrete log splits;
 * this reader subscribes those buckets in fluss-rs and drains Arrow {@code RecordBatch}es directly.
 */
final class NativeFlussSplitReader implements SplitReader<NativeSourceRecord, SourceSplitBase> {

  private static final long NO_PARTITION = Long.MIN_VALUE;
  private static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;

  private final long handle;
  private final long pollTimeoutMillis;
  private final BufferAllocator allocator = NativeAllocator.SHARED;
  private final BoundedSplitTracker<NativeFlussLogSplit> tracker = new BoundedSplitTracker<>();
  private final NativeFlussSourceMetrics metrics;

  NativeFlussSplitReader(
      String[] configKeys,
      String[] configValues,
      String databaseName,
      String tableName,
      int[] projectedFields,
      int rowtimeIndex,
      long pollTimeoutMillis) {
    this(
        configKeys,
        configValues,
        databaseName,
        tableName,
        projectedFields,
        rowtimeIndex,
        pollTimeoutMillis,
        null);
  }

  NativeFlussSplitReader(
      String[] configKeys,
      String[] configValues,
      String databaseName,
      String tableName,
      int[] projectedFields,
      int rowtimeIndex,
      long pollTimeoutMillis,
      NativeFlussSourceMetrics metrics) {
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.metrics = metrics;
    this.handle =
        NativeFluss.openFlussReader(
            configKeys, configValues, databaseName, tableName, projectedFields, rowtimeIndex);
  }

  @Override
  public RecordsWithSplitIds<NativeSourceRecord> fetch() {
    Set<String> pendingFinished = tracker.drainPendingFinished();
    if (!pendingFinished.isEmpty()) {
      RecordsBySplits.Builder<NativeSourceRecord> finishedBuilder = new RecordsBySplits.Builder<>();
      finishedBuilder.addFinishedSplits(pendingFinished);
      return finishedBuilder.build();
    }
    int pending = NativeFluss.pollFlussBatch(handle, pollTimeoutMillis);
    RecordsBySplits.Builder<NativeSourceRecord> builder = new RecordsBySplits.Builder<>();
    for (int i = 0; i < pending; i++) {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        long[] meta = new long[2];
        String[] splitId = new String[1];
        NativeFluss.drainFlussSplit(
            handle, meta, splitId, outArray.memoryAddress(), outSchema.memoryAddress());
        VectorSchemaRoot root =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        tracker.recordPosition(splitId[0], meta[0]);
        if (metrics != null) {
          metrics.recordBatch(splitId[0], meta[0], root.getRowCount(), meta[1]);
        }
        builder.add(splitId[0], new NativeSourceRecord(new ArrowBatch(root), meta[0], meta[1]));
      }
    }

    List<NativeFlussLogSplit> justFinished = tracker.finishReached(builder::addFinishedSplit);
    if (!justFinished.isEmpty()) {
      unassign(justFinished);
    }
    return builder.build();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChanges) {
    if (splitsChanges instanceof SplitsRemoval) {
      removeSplits(splitsChanges.splits());
      return;
    }
    List<SourceSplitBase> splits = splitsChanges.splits();
    List<NativeFlussLogSplit> nativeSplits = new ArrayList<>(splits.size());
    for (SourceSplitBase split : splits) {
      NativeFlussLogSplit nativeSplit = FlussSplitTranslator.translateLogSplit(split);
      OptionalLong stoppingOffset = nativeSplit.stoppingOffset();
      if (stoppingOffset.isPresent() && nativeSplit.startingOffset() >= stoppingOffset.getAsLong()) {
        tracker.markPendingFinished(nativeSplit.splitId());
        continue;
      }
      nativeSplits.add(nativeSplit);
      if (metrics != null) {
        metrics.register(nativeSplit);
      }
      tracker.track(
          nativeSplit.splitId(), nativeSplit, nativeSplit.startingOffset(), stoppingOffset);
    }
    if (!nativeSplits.isEmpty()) {
      NativeFluss.assignFlussSplits(
          handle,
          strings(nativeSplits, NativeFlussLogSplit::splitId),
          longs(nativeSplits, NativeFlussLogSplit::tableId),
          longs(nativeSplits, split -> split.partitionId().orElse(NO_PARTITION)),
          longs(nativeSplits, NativeFlussLogSplit::bucket),
          longs(nativeSplits, NativeFlussLogSplit::startingOffset),
          longs(nativeSplits, split -> split.stoppingOffset().orElse(NO_STOPPING_OFFSET)));
    }
  }

  /**
   * Unsubscribes every assigned split belonging to the removed partitions and returns their table
   * buckets for the coordinator ack — the split reader answers the "which splits" question from
   * its own bookkeeping, exactly as Fluss's {@code FlinkSourceSplitReader.removePartitions} does.
   * The removed splits are reported as finished on the next {@link #fetch()} so the source reader
   * drops them from checkpoint state.
   */
  Set<TableBucket> removePartitions(Map<Long, String> removedPartitions) {
    List<NativeFlussLogSplit> removed = new ArrayList<>();
    Set<TableBucket> buckets = new HashSet<>();
    for (NativeFlussLogSplit split : tracker.trackedSplits()) {
      OptionalLong partitionId = split.partitionId();
      if (partitionId.isPresent() && removedPartitions.containsKey(partitionId.getAsLong())) {
        removed.add(split);
        buckets.add(new TableBucket(split.tableId(), partitionId.getAsLong(), split.bucket()));
      }
    }
    for (NativeFlussLogSplit split : removed) {
      tracker.retire(split.splitId());
    }
    if (!removed.isEmpty()) {
      unassign(removed);
    }
    return buckets;
  }

  private void removeSplits(List<SourceSplitBase> splits) {
    List<NativeFlussLogSplit> nativeSplits = new ArrayList<>(splits.size());
    for (SourceSplitBase split : splits) {
      NativeFlussLogSplit nativeSplit = tracker.retire(split.splitId());
      if (nativeSplit == null) {
        nativeSplit = FlussSplitTranslator.translateLogSplit(split);
      }
      nativeSplits.add(nativeSplit);
    }
    if (!nativeSplits.isEmpty()) {
      unassign(nativeSplits);
    }
  }

  @Override
  public void wakeUp() {
    // Native poll uses a short bounded timeout; no interrupt is needed.
  }

  @Override
  public void close() {
    NativeFluss.closeFlussReader(handle);
  }

  private void unassign(List<NativeFlussLogSplit> splits) {
    NativeFluss.unassignFlussSplits(
        handle,
        longs(splits, NativeFlussLogSplit::tableId),
        longs(splits, split -> split.partitionId().orElse(NO_PARTITION)),
        longs(splits, NativeFlussLogSplit::bucket));
  }

  private static long[] longs(
      List<NativeFlussLogSplit> splits, ToLongFunction<NativeFlussLogSplit> field) {
    long[] values = new long[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = field.applyAsLong(splits.get(i));
    }
    return values;
  }

  private static String[] strings(
      List<NativeFlussLogSplit> splits, Function<NativeFlussLogSplit, String> field) {
    String[] values = new String[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      values[i] = field.apply(splits.get(i));
    }
    return values;
  }
}
