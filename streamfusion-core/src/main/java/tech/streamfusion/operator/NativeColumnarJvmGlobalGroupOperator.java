package tech.streamfusion.operator;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGlobalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.types.logical.RowType;

/** Merges decoded local partials into the released runtime's keyed accumulator/DataView state. */
public final class NativeColumnarJvmGlobalGroupOperator extends NativeColumnarJvmGroupOperator {
  public NativeColumnarJvmGlobalGroupOperator(
      RowType input,
      RowType output,
      MiniBatchGlobalGroupAggFunction function,
      BundleTrigger<RowData> trigger,
      RowDataKeySelector selector,
      int batchRows,
      RowType partial,
      long retention) {
    super(input, output, function, trigger, selector, batchRows, null, -1, retention);
    withPartialTransport(true, partial, null);
  }
}
