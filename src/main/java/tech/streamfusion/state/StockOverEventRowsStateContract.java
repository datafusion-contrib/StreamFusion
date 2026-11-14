package tech.streamfusion.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** Released event-time bounded ROWS arithmetic, retained input and cleanup state. */
public final class StockOverEventRowsStateContract extends StockBoundedOverStateContract {
  private static final long serialVersionUID = 1L;

  public StockOverEventRowsStateContract(
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
      byte[] constantBytes,
      long precedingRows) {
    this(keys, keyType, inputType, accumulatorType, keyColumns, timeColumn, kinds, widths,
        hiddenCount, constants, constantBytes, precedingRows, false);
  }

  public StockOverEventRowsStateContract(
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
      byte[] constantBytes,
      long precedingRows,
      boolean cleanupEnabled) {
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
        precedingRows,
        false,
        true,
        cleanupEnabled);
  }
}
