package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import org.apache.flink.table.data.RowData;

/**
 * Single-phase window aggregation: the planner substitutes this for a tumbling, hopping, or
 * cumulative window aggregate. Input rows carry an event-time column and the value to aggregate (and
 * optionally a grouping key); output rows carry the key, the aggregate, and the window bounds, in
 * the order the host produces. Cumulative windows bind to the cumulative native aggregator, where
 * {@code windowMillis}/{@code slideMillis} are the max size and the step.
 */
public class NativeWindowAggregateOperator extends NativeWindowOperatorBase {

  private final boolean cumulative;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  public NativeWindowAggregateOperator(
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
      int batchSize) {
    super(
        "streamfusion-window-aggregate-state",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId,
        batchSize);
    this.cumulative = cumulative;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
  }

  @Override
  protected long createHandle() {
    return cumulative
        ? Native.createCumulativeAggregator(windowMillis, slideMillis, valueTypes, aggregateKinds)
        : super.createHandle();
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return cumulative
        ? Native.restoreCumulativeAggregator(
            windowMillis, slideMillis, valueTypes, aggregateKinds, snapshot)
        : super.restoreHandle(snapshot);
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    updateRaw(rows, timeColumn, valueColumns, keyColumns, keyTypes);
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyTypes);
  }
}
