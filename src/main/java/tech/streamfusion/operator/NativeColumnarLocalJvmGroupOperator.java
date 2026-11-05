package tech.streamfusion.operator;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchLocalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.types.logical.RowType;

/** Local UDF bundles have no retained state and drain their stock partials before barriers. */
public final class NativeColumnarLocalJvmGroupOperator extends NativeColumnarJvmGroupOperator {
  public NativeColumnarLocalJvmGroupOperator(
      RowType inputType,
      RowType outputType,
      MiniBatchLocalGroupAggFunction function,
      BundleTrigger<RowData> trigger,
      RowDataKeySelector selector,
      int batchRows,
      JvmGroupPartialTransport transport) {
    super(
        inputType,
        outputType,
        null,
        function,
        trigger,
        selector,
        batchRows,
        0,
        null,
        -1,
        0,
        null,
        transport);
  }
}
