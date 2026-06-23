package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * Columnar single-phase window aggregation: the same native aggregator as {@link
 * NativeWindowAggregateOperator}, but fed Arrow batches directly instead of buffered rows. The
 * planner substitutes this when the window's keyed input is kept columnar across the exchange, so
 * the data never transposes to {@link RowData} on the way in. Output is still rows ({@code [key?,
 * agg…, window_start, window_end]}), so a row consumer downstream needs no transpose.
 */
public class NativeColumnarWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, RowData> {

  private final boolean cumulative;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

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
      String timeZoneId) {
    super(
        "streamfusion-window-aggregate-state",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId);
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
  public void processElement(StreamRecord<ArrowBatch> element) {
    try (VectorSchemaRoot in = element.getValue().root()) {
      updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
    }
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
