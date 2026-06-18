package io.github.jordepic.streamfusion.operator;

import java.util.List;
import org.apache.flink.table.data.RowData;

/**
 * Single-phase window aggregation: the planner substitutes this for a tumbling-window aggregate.
 * Input rows carry an event-time column and the value to aggregate (and optionally a grouping key);
 * output rows carry the key, the aggregate, and the window bounds, in the order the host produces.
 */
public class NativeWindowAggregateOperator extends NativeWindowOperatorBase {

  private final int timeColumn;
  private final int valueColumn;
  private final int keyColumn;

  public NativeWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int valueColumn,
      int keyColumn,
      int valueType,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    super(
        "streamfusion-window-aggregate-state",
        windowMillis,
        slideMillis,
        valueType,
        aggregateKinds,
        timeZoneId,
        batchSize);
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumn = keyColumn;
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    updateRaw(rows, timeColumn, valueColumn, keyColumn);
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyColumn);
  }
}
