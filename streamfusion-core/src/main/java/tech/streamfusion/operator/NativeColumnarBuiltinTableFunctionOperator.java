package tech.streamfusion.operator;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedClass;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.types.logical.RowType;

/** Preserves the bytes and Java string caches observed by built-in callbacks. */
public final class NativeColumnarBuiltinTableFunctionOperator
    extends NativeColumnarTableFunctionOperator {
  private static final long serialVersionUID = 1L;

  public NativeColumnarBuiltinTableFunctionOperator(
      RowType inputType,
      RowType outputType,
      GeneratedClass<? extends StreamOperator<RowData>> generated,
      int[] projection,
      int batchRows,
      GeneratedProjection resultProjection) {
    super(inputType, outputType, generated, projection, batchRows, false, null, resultProjection);
  }

  @Override
  protected boolean preservesInternalStrings() {
    return true;
  }
}
