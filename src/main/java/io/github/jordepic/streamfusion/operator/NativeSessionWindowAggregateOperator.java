package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import org.apache.flink.table.data.RowData;

/**
 * Single-phase session-window aggregation: the planner substitutes this for a session-window
 * aggregate. It reuses the shared window scaffolding but is backed by the native session aggregator,
 * whose dynamic per-key windows merge on the inactivity gap and so carry their own end bound.
 */
public class NativeSessionWindowAggregateOperator extends NativeWindowOperatorBase {

  private final long gapMillis;
  private final int timeColumn;
  private final int valueColumn;
  private final int[] keyColumns;
  private final int[] keyTypes;

  public NativeSessionWindowAggregateOperator(
      long gapMillis,
      int timeColumn,
      int valueColumn,
      int[] keyColumns,
      int[] keyTypes,
      int valueType,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    // Sessions have no fixed size or slide; the gap is the only window parameter, carried separately.
    super(
        "streamfusion-session-aggregate-state",
        0,
        0,
        valueType,
        aggregateKinds,
        timeZoneId,
        batchSize);
    this.gapMillis = gapMillis;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
  }

  @Override
  protected long createHandle() {
    return Native.createSessionAggregator(gapMillis, valueType, aggregateKinds);
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return Native.restoreSessionAggregator(gapMillis, valueType, aggregateKinds, snapshot);
  }

  @Override
  protected void updateHandle(long arrayAddress, long schemaAddress) {
    Native.updateSessionAggregator(handle, arrayAddress, schemaAddress);
  }

  @Override
  protected void flushHandle(long watermark, long arrayAddress, long schemaAddress) {
    Native.flushSessionAggregator(handle, watermark, arrayAddress, schemaAddress);
  }

  @Override
  protected byte[] snapshotHandle() {
    return Native.snapshotSessionAggregator(handle);
  }

  @Override
  protected void closeHandle() {
    Native.closeSessionAggregator(handle);
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    updateRaw(rows, timeColumn, valueColumn, keyColumns, keyTypes);
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyTypes);
  }
}
