package tech.streamfusion.operator;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchLocalGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.types.logical.RowType;

/** Local partials have no keyed state; their complex fields use released Flink serializers. */
public final class NativeColumnarJvmLocalGroupOperator extends NativeColumnarJvmGroupOperator {
  public NativeColumnarJvmLocalGroupOperator(
      RowType input,
      RowType output,
      MiniBatchLocalGroupAggFunction function,
      BundleTrigger<RowData> trigger,
      RowDataKeySelector selector,
      int batchRows,
      RowType partial) {
    super(input, output, function, trigger, selector, batchRows, null, -1, 0);
    withPartialTransport(false, null, partial);
  }
}
