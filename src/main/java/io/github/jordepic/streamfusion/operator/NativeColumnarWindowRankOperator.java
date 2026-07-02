package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar window Top-N / window deduplication over a windowing-TVF input (the host's {@code
 * WindowRank}/{@code WindowDeduplicate}): Arrow in, Arrow out. Within each window (the attached
 * {@code window_start}/{@code window_end} columns) and partition key, the native ranker keeps the
 * top {@code limit} rows by the sort columns and emits them when a watermark closes the window —
 * append-only, emitted once. Deduplication is the {@code limit = 1} case. The buffering, ranking,
 * and late-row drop live in the native ranker; this layer moves batches across the bridge and owns
 * the handle's checkpointed state.
 *
 * <p>An event-time window rank closes windows on a watermark. A **proctime** window rank closes them
 * on the processing-time clock: the upstream proctime TVF assigns each row to the window covering the
 * clock, and this operator fires a processing-time timer at each window end (chaining to the next
 * slide boundary while windows remain open, like the proctime window aggregate). It ignores
 * watermarks in that mode and drains on finish.
 */
public class NativeColumnarWindowRankOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final int windowStartColumn;
  private final int windowEndColumn;
  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long limit;
  private final boolean outputRankNumber;
  private final String timeZoneId;
  private final boolean proctime;
  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient ZoneId zone;
  private transient long handle;
  private transient ListState<byte[]> handleState;
  private transient ManagedMemoryBudget memoryBudget;
  private transient long registeredTimer;
  private transient long maxOpenEnd;

  public NativeColumnarWindowRankOperator(
      int windowStartColumn,
      int windowEndColumn,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      String timeZoneId,
      boolean proctime,
      long windowMillis,
      long slideMillis,
      boolean cumulative) {
    this.windowStartColumn = windowStartColumn;
    this.windowEndColumn = windowEndColumn;
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
    this.timeZoneId = timeZoneId;
    this.proctime = proctime;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-window-rank-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    handle =
        snapshot == null
            ? Native.createWindowRanker(
                windowStartColumn,
                windowEndColumn,
                partitionColumns,
                sortIndices,
                sortAscending,
                sortNullsFirst,
                limit,
                outputRankNumber,
                memoryBudget.bytes())
            : Native.restoreWindowRanker(
                windowStartColumn,
                windowEndColumn,
                partitionColumns,
                sortIndices,
                sortAscending,
                sortNullsFirst,
                limit,
                outputRankNumber,
                snapshot,
                memoryBudget.bytes());
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    zone = ZoneId.of(timeZoneId);
    registeredTimer = Long.MIN_VALUE;
    maxOpenEnd = Long.MIN_VALUE;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      Native.pushWindowRanker(handle, array.memoryAddress(), schema.memoryAddress());
    } finally {
      in.close();
    }
    if (proctime) {
      long now = getProcessingTimeService().getCurrentProcessingTime();
      flush(now);
      maxOpenEnd = Math.max(maxOpenEnd, latestWindowEnd(now));
      scheduleNextTimer(now);
    }
    publishStateBytes();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // Proctime ranks close on the processing-time clock, not the watermark; just forward it.
    if (!proctime) {
      flush(mark.getTimestamp());
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  @Override
  public void onProcessingTime(long time) {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    flush(now);
    scheduleNextTimer(now);
    publishStateBytes();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.windowRankerStateBytes(handle));
    }
  }

  @Override
  public void finish() throws Exception {
    if (proctime) {
      flush(Long.MAX_VALUE); // end of input: close every remaining window
    }
    super.finish();
  }

  private void scheduleNextTimer(long now) {
    long boundary = Math.floorDiv(now, slideMillis) * slideMillis + slideMillis;
    if (boundary <= maxOpenEnd && boundary > registeredTimer) {
      getProcessingTimeService().registerTimer(boundary, this);
      registeredTimer = boundary;
    }
  }

  private long latestWindowEnd(long now) {
    return cumulative
        ? Math.floorDiv(now, windowMillis) * windowMillis + windowMillis
        : Math.floorDiv(now, slideMillis) * slideMillis + windowMillis;
  }

  /** Emits and evicts every window whose end the given threshold has passed. */
  private void flush(long threshold) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushWindowRanker(handle, threshold, array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        // The native side keeps window_start/window_end as UTC epoch (so eviction compares against the
        // UTC threshold); render them as session-local wall-clock TIMESTAMPs on emit, as the host does
        // (window_time stays the UTC rowtime). Same toLocal shift as the window aggregate.
        shiftToLocal(out, windowStartColumn);
        shiftToLocal(out, windowEndColumn);
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no window closed at this threshold
      }
    }
  }

  /** Rewrites a UTC-epoch timestamp column to the session-local wall-clock the host emits. */
  private void shiftToLocal(VectorSchemaRoot out, int column) {
    if (!(out.getVector(column) instanceof TimeStampNanoVector)) {
      return;
    }
    TimeStampNanoVector ts = (TimeStampNanoVector) out.getVector(column);
    for (int i = 0; i < out.getRowCount(); i++) {
      if (ts.isNull(i)) {
        continue;
      }
      long utcMillis = ts.get(i) / 1_000_000L;
      long localMillis =
          Instant.ofEpochMilli(utcMillis).atZone(zone).toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
      ts.setSafe(i, localMillis * 1_000_000L);
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotWindowRanker(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeWindowRanker(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
