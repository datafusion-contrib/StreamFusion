package tech.streamfusion.operator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.state.RocksDBNativeStateSupport;

/**
 * Columnar single-phase session-window aggregation over Arrow batches.
 *
 * <p>Regional LTZ sessions measure connectivity in local time and convert each window end through
 * Flink's calendar timer contract. Other sessions retain their epoch assignment domain. Event-time
 * sessions fire on watermarks; processing-time sessions use the operator's clock. Each firing pass
 * records the earliest remaining deadline, which is rearmed after firing and reconstructed from
 * restored state. Bounded-input completion does not close an unfinished processing-time session.
 */
public class NativeColumnarSessionWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final long gapMillis;
  private final String sessionCalendarZoneId;
  private final boolean proctime;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  /** The earliest open-session timer currently scheduled. */
  private transient long registeredTimer;

  private transient java.time.ZoneId calendarZone;

  @Override
  protected boolean isEventTimeWindow() {
    return !proctime;
  }

  public NativeColumnarSessionWindowAggregateOperator(
      long gapMillis,
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
        gapMillis, timeColumn, valueColumns, keyColumns, keyTypes, valueTypes, aggregateKinds,
        timeZoneId, !"UTC".equals(timeZoneId), timeZoneId, outputType, proctime,
        keyTimestampPrecisions, maxParallelism);
  }

  public NativeColumnarSessionWindowAggregateOperator(
      long gapMillis,
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
    // Sessions have no fixed size or slide; the gap is the only window parameter, carried
    // separately.
    super(
        "session aggregate",
        0,
        0,
        valueTypes,
        aggregateKinds,
        timeZoneId,
        timestampLtz,
        sessionTimeZoneId,
        outputType,
        keyTimestampPrecisions,
        maxParallelism);
    this.gapMillis = gapMillis;
    this.sessionCalendarZoneId = timeZoneId;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
    this.proctime = proctime;
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (inputTimestampLtz) {
      var configuredZone = java.time.ZoneId.of(sessionCalendarZoneId);
      if (!configuredZone.getRules().isFixedOffset()) {
        calendarZone = configuredZone;
        Native.setSessionWindowCalendar(
            handle, new WindowCalendar(sessionCalendarZoneId, proctime));
      }
    }
    registeredTimer = Long.MIN_VALUE;
    if (proctime) {
      // Reconstruct the earliest remaining deadline, including clipped/rescaled state sources.
      emitClosedWindows(Long.MIN_VALUE);
      long now = getProcessingTimeService().getCurrentProcessingTime();
      long next = Native.sessionAggregatorNextTimerDeadline(handle);
      if (next != Long.MIN_VALUE && next <= now) emitClosedWindows(now);
      registerNextTimer();
    }
  }

  // Proctime sessions share the event-time state layout (rows are stamped with the clock and the
  // gap closes by timer instead of watermark); the cleanup deadline rides the typed store's
  // reserved key, so both modes run on the direct store.
  @Override
  protected boolean usesDirectRocksDBState() {
    return Native.rocksdbSessionAggregatorSupported(valueTypes, aggregateKinds, keyTypes);
  }

  @Override
  protected long createRocksDBHandle(
      RocksDBNativeStateSupport rocksdb, byte[][] restoredPartitions) {
    return Native.createRocksDBSessionAggregator(
        gapMillis, valueTypes, aggregateKinds, keyTypes, keyTimestampPrecisions(),
        memoryBudgetBytes(), rocksdb.tableDirectory(), maxParallelism(), rocksdb.optionsJson(),
        rocksdb.sharedResourcesHandle(), rocksdb.sourceDirectories(),
        rocksdb.sourceSnapshotTokens(), rocksdb.keyGroupStart(), rocksdb.keyGroupEnd(),
        rocksdb.aligned(), restoredPartitions,
        restoredProcessingTimeTimerDeadline());
  }

  @Override
  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    return directRocksDBState()
        ? Native.checkpointRocksDBSessionAggregator(
            handle, processingTimeTimerDeadlineForSnapshot(), snapshotDirectory)
        : super.checkpointRocksDBHandle(snapshotDirectory);
  }

  @Override
  protected long rocksdbTimerDeadline() {
    return Native.rocksdbSessionAggregatorTimerDeadline(handle);
  }

  @Override
  protected byte[][] snapshotCanonicalPartitions() {
    return directRocksDBState()
        ? Native.snapshotRocksDBSessionAggregatorPartitions(
            handle, maxParallelism(), keyTimestampPrecisions())
        : snapshotRawPartitions();
  }

  @Override
  protected boolean usesAlignedWindowAssignment() {
    return false;
  }

  @Override
  protected long assignmentTimestampMillis(long epochMillis) {
    return calendarZone == null
        ? epochMillis
        : org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills(
            epochMillis, calendarZone);
  }

  @Override
  protected boolean usesLocalWindowBounds() {
    return calendarZone != null;
  }

  @Override
  protected long createHandle() {
    return Native.createSessionAggregator(gapMillis, valueTypes, aggregateKinds, memoryBudgetBytes());
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return Native.restoreSessionAggregator(
        gapMillis, valueTypes, aggregateKinds, snapshot, memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreSessionAggregatorPartitions(
        gapMillis, valueTypes, aggregateKinds, snapshots, memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotSessionAggregatorPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void updateHandle(long arrayAddress, long schemaAddress) {
    if (directRocksDBState()) {
      Native.pushRocksDBSessionAggregator(handle, arrayAddress, schemaAddress);
    } else {
      Native.updateSessionAggregator(handle, arrayAddress, schemaAddress);
    }
  }

  @Override
  protected void flushHandle(long watermark, long arrayAddress, long schemaAddress) {
    watermark = WindowTimeDomain.closeThreshold(watermark, 0);
    if (directRocksDBState()) {
      Native.flushRocksDBSessionAggregator(handle, watermark, arrayAddress, schemaAddress);
    } else {
      Native.flushSessionAggregator(handle, watermark, arrayAddress, schemaAddress);
    }
  }

  @Override
  protected byte[] snapshotHandle() {
    return Native.snapshotSessionAggregator(handle);
  }

  @Override
  protected void closeHandle() {
    if (directRocksDBState()) {
      Native.closeRocksDBSessionAggregator(handle);
    } else {
      Native.closeSessionAggregator(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return directRocksDBState()
        ? Native.rocksdbSessionAggregatorStateBytes(handle)
        : Native.sessionAggregatorStateBytes(handle);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    try (VectorSchemaRoot in = element.getValue().root()) {
      if (proctime) {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        updateColumnarProctime(in, now, valueColumns, keyColumns, keyTypes);
        emitClosedWindows(now); // emit any session the clock has already left behind by a full gap
        registerNextTimer();
      } else {
        updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
        reportLateRecords(Native.sessionAggregatorLateDrops(handle));
      }
    }
    publishStateBytes();
  }

  @Override
  public void onProcessingTime(long time) {
    emitClosedWindows(getProcessingTimeService().getCurrentProcessingTime());
    registerNextTimer();
  }

  private void registerNextTimer() {
    long deadline = Native.sessionAggregatorNextTimerDeadline(handle);
    if (deadline != Long.MIN_VALUE && deadline != registeredTimer) {
      getProcessingTimeService().registerTimer(deadline, this);
    }
    registeredTimer = deadline;
  }

  @Override
  protected long processingTimeTimerDeadlineForSnapshot() {
    return proctime ? registeredTimer : Long.MIN_VALUE;
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
