package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.PaimonNativeStateSupport;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

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
public class NativeColumnarWindowRankOperator extends AbstractNativeStatefulOperator<ArrowBatch>
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
  private final RowType rowType;

  private transient ZoneId zone;
  private transient long registeredTimer;
  private transient long maxOpenEnd;
  private transient FlinkWindowMetrics flinkWindowMetrics;

  public NativeColumnarWindowRankOperator(
      int windowStartColumn,
      int windowEndColumn,
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      String timeZoneId,
      boolean proctime,
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      RowType rowType,
      int maxParallelism) {
    super("window rank", keyTimestampPrecisions, maxParallelism);
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
    this.rowType = rowType;
  }

  // A proctime window rank closes windows on processing-time timers, so the deadline must travel
  // in every raw key group; an event-time one writes the frame with no deadline.
  @Override
  protected boolean carriesProcessingTimeTimer() {
    return true;
  }

  @Override
  protected long processingTimeTimerDeadlineForSnapshot() {
    return proctime ? maxOpenEnd : Long.MIN_VALUE;
  }

  @Override
  protected PaimonNativeStateSupport resolvePaimonState(boolean rawStateRestored) {
    // A proctime window rank keeps memory state under the Paimon backend too: it closes windows
    // on processing-time timers whose deadline travels in raw state, not on watermarks.
    if (proctime) {
      return null;
    }
    return resolvePaimon(
        rawStateRestored,
        () -> withRowSchema(rowType, address -> Native.paimonRowStateSupported(address) ? 1L : 0L) != 0);
  }

  @Override
  protected long createPaimonHandle(PaimonNativeStateSupport paimon) {
    return withRowSchema(
        rowType,
        rowSchemaAddress ->
            Native.createPaimonWindowRanker(
                windowStartColumn,
                windowEndColumn,
                partitionColumns,
                keyTimestampPrecisions(),
                sortIndices,
                sortAscending,
                sortNullsFirst,
                limit,
                outputRankNumber,
                rowSchemaAddress,
                memoryBudgetBytes(),
                paimon.tableDirectory(),
                maxParallelism(),
                NativeConfig.paimonBuckets(),
                NativeConfig.paimonFileFormat(),
                NativeConfig.paimonFileCompression(),
                paimon.sourceDirectories(),
                paimon.sourceSnapshotTokens(),
                paimon.keyGroupStart(),
                paimon.keyGroupEnd(),
                paimon.aligned()));
  }

  @Override
  protected String[] checkpointPaimonHandle() {
    return Native.checkpointPaimonWindowRanker(handle);
  }

  @Override
  protected long createHandle() {
    return Native.createWindowRanker(
        windowStartColumn,
        windowEndColumn,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit,
        outputRankNumber,
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreWindowRankerPartitions(
        windowStartColumn,
        windowEndColumn,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit,
        outputRankNumber,
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotWindowRankerPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    if (paimonState()) {
      Native.closePaimonWindowRanker(handle);
    } else {
      Native.closeWindowRanker(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return paimonState()
        ? Native.paimonWindowRankerStateBytes(handle)
        : Native.windowRankerStateBytes(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    flinkWindowMetrics =
        new FlinkWindowMetrics(getMetricGroup(), getProcessingTimeService());
    zone = ZoneId.of(timeZoneId);
    registeredTimer = Long.MIN_VALUE;
    maxOpenEnd = restoredProcessingTimeTimerDeadline();
    if (proctime && maxOpenEnd != Long.MIN_VALUE) {
      long now = getProcessingTimeService().getCurrentProcessingTime();
      if (maxOpenEnd <= now) {
        flush(now);
      } else {
        scheduleNextTimer(now);
      }
    }
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      if (paimonState()) {
        Native.pushPaimonWindowRanker(handle, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.pushWindowRanker(handle, array.memoryAddress(), schema.memoryAddress());
      }
    } finally {
      in.close();
    }
    flinkWindowMetrics.reportLateRecords(
        paimonState()
            ? Native.paimonWindowRankerLateDrops(handle)
            : Native.windowRankerLateDrops(handle));
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
    if (!proctime) {
      flinkWindowMetrics.onWatermark(mark.getTimestamp());
    }
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
      if (paimonState()) {
        Native.flushPaimonWindowRanker(
            handle, threshold, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.flushWindowRanker(
            handle, threshold, array.memoryAddress(), schema.memoryAddress());
      }
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        // The native side keeps window_start/window_end as UTC epoch (so eviction compares against the
        // UTC threshold); render them as session-local wall-clock TIMESTAMPs on emit, as the host does
        // (window_time stays the UTC rowtime). Same toLocal shift as the window aggregate.
        shiftToLocal(out, windowStartColumn);
        shiftToLocal(out, windowEndColumn);
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
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

}
