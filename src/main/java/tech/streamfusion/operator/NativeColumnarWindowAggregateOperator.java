package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.RocksDBNativeStateSupport;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar single-phase window aggregation: the same native aggregator as {@link
 * NativeWindowAggregateOperator}, but fed Arrow batches directly instead of buffered rows, and
 * emitting Arrow batches ({@code [key?, agg…, window_start, window_end]}). The whole operator is
 * Arrow → Arrow; a rowwise sink downstream is reached through the dedicated
 * {@code ArrowToRowDataOperator} the planner inserts at the island perimeter.
 *
 * <p>Event-time windows assign each row by its rowtime column and fire on a watermark (the core's
 * default). A **proctime** window instead assigns every row in a batch to the window(s) covering the
 * operator's current processing time (Flink's processing-time assigner uses the clock, not a row
 * value) and fires on a processing-time timer at each window's end — so a closed window is emitted
 * when wall-clock passes its boundary even with no further input. Hopping and cumulative windows leave
 * several windows open at once, so the timer chains: each firing schedules the next slide boundary
 * until the clock has passed the latest open window's end. Remaining open windows are flushed when
 * the (bounded) input finishes.
 */
public class NativeColumnarWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final boolean cumulative;
  private final boolean proctime;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  /** The latest processing-time boundary already scheduled, so each window-end timer registers once. */
  private transient long registeredTimer;

  /** The latest end of any window that has received a row, so the timer stops chaining once drained. */
  private transient long maxOpenEnd;

  @Override
  protected boolean isEventTimeWindow() {
    return !proctime;
  }

  public NativeColumnarWindowAggregateOperator(
      boolean cumulative,
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      RowType outputType,
      boolean proctime,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    this(
        cumulative, windowMillis, slideMillis, timeColumn, valueColumns, keyColumns, keyTypes,
        valueTypes, aggregateKinds, timeZoneId, !"UTC".equals(timeZoneId), timeZoneId, outputType,
        proctime, keyTimestampPrecisions, maxParallelism);
  }

  public NativeColumnarWindowAggregateOperator(
      boolean cumulative,
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      boolean timestampLtz,
      String sessionTimeZoneId,
      RowType outputType,
      boolean proctime,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    super(
        "window aggregate",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId,
        timestampLtz,
        sessionTimeZoneId,
        outputType,
        keyTimestampPrecisions,
        maxParallelism);
    this.cumulative = cumulative;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
    this.proctime = proctime;
  }

  @Override
  public void open() throws Exception {
    super.open();
    registeredTimer = Long.MIN_VALUE;
    maxOpenEnd = restoredProcessingTimeTimerDeadline();
    if (proctime && maxOpenEnd != Long.MIN_VALUE) {
      long now = getProcessingTimeService().getCurrentProcessingTime();
      if (maxOpenEnd <= now) {
        emitClosedWindows(now);
      } else {
        scheduleNextTimer(now);
      }
    }
  }

  // Proctime windows share the event-time state layout (rows are stamped with the clock and fire
  // by timer instead of watermark); the firing deadline rides the typed store's reserved key, so
  // both modes run on the direct store.
  @Override
  protected boolean usesDirectRocksDBState() {
    return Native.rocksdbWindowAggregatorSupported(valueTypes, aggregateKinds, keyTypes);
  }

  @Override
  protected long createRocksDBHandle(RocksDBNativeStateSupport rocksdb) {
    return createRocksDBWindowAggregatorHandle(rocksdb, cumulative, keyTypes);
  }

  @Override
  protected long createHandle() {
    return cumulative
        ? Native.createCumulativeAggregator(
            windowMillis, slideMillis, valueTypes, aggregateKinds, memoryBudgetBytes())
        : super.createHandle();
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return cumulative
        ? Native.restoreCumulativeAggregator(
            windowMillis, slideMillis, valueTypes, aggregateKinds, snapshot, memoryBudgetBytes())
        : super.restoreHandle(snapshot);
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreTumblingAggregatorPartitions(
        windowMillis,
        slideMillis,
        cumulative,
        valueTypes,
        aggregateKinds,
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    try (var in = element.getValue().root()) {
      if (proctime) {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        updateColumnarProctime(in, now, valueColumns, keyColumns, keyTypes);
        emitClosedWindows(now); // emit windows the clock has already passed
        maxOpenEnd = Math.max(maxOpenEnd, latestWindowEnd(now));
        scheduleNextTimer(now);
      } else {
        updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
        reportLateRecords(Native.tumblingAggregatorLateDrops(handle));
      }
    }
    publishStateBytes();
  }

  @Override
  public void onProcessingTime(long time) {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    emitClosedWindows(now);
    scheduleNextTimer(now); // chain to the next slide boundary while windows remain open
    publishStateBytes();
  }

  @Override
  protected long processingTimeTimerDeadlineForSnapshot() {
    return proctime ? maxOpenEnd : Long.MIN_VALUE;
  }

  /**
   * Register a timer at the next window-end boundary strictly after {@code now}, unless every open
   * window is already drained or that boundary is already scheduled. Window ends fall on slide
   * boundaries (the matcher requires the slide to divide the size), so the next end is the next slide
   * multiple. Processing time only advances, so the latest boundary scheduled never needs revisiting.
   */
  private void scheduleNextTimer(long now) {
    long boundary = Math.floorDiv(now, slideMillis) * slideMillis + slideMillis;
    if (boundary <= maxOpenEnd && boundary > registeredTimer) {
      getProcessingTimeService().registerTimer(boundary, this);
      registeredTimer = boundary;
    }
  }

  /**
   * The latest end of any window covering processing time {@code now}: for cumulative windows the end
   * of the enclosing max-size window; for tumbling/hopping the end of the window with the latest start
   * that still covers {@code now}. Bounds how far the timer keeps chaining after the last row.
   */
  private long latestWindowEnd(long now) {
    return cumulative
        ? Math.floorDiv(now, windowMillis) * windowMillis + windowMillis
        : Math.floorDiv(now, slideMillis) * slideMillis + windowMillis;
  }

  @Override
  public void finish() throws Exception {
    if (proctime) {
      emitClosedWindows(Long.MAX_VALUE); // end of input: close every remaining window
    }
    super.finish();
  }

  @Override
  protected void flushPending() {
    // Each batch is folded into the aggregator as it arrives; nothing is buffered here.
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyTypes);
  }
}
