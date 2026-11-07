package tech.streamfusion.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** Released bounded event-time RANGE state with raw arithmetic and Void timers. */
public final class StockOverRangeStateContract extends StockBoundedOverStateContract {
  private static final long serialVersionUID = 1L;

  public StockOverRangeStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType inputType,
      RowType accumulatorType,
      int[] keyColumns,
      int timeColumn,
      int[] kinds,
      int[] widths,
      int hiddenCount,
      RowType constants,
      byte[] constantBytes) {
    super(
        keys,
        keyType,
        inputType,
        accumulatorType,
        keyColumns,
        timeColumn,
        kinds,
        widths,
        hiddenCount,
        constants,
        constantBytes,
        0,
        false,
        false,
        true);
  }
}
