package tech.streamfusion.compat;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;

/** Released aggregate lifecycle and bundle creation for this Flink line. */
public final class JvmAggregateCompat {
  private JvmAggregateCompat() {}

  public static void openOver(
      org.apache.flink.streaming.api.functions.KeyedProcessFunction<
              org.apache.flink.table.data.RowData,
              org.apache.flink.table.data.RowData,
              org.apache.flink.table.data.RowData>
          function,
      RuntimeContext context)
      throws Exception {
    function.setRuntimeContext(context);
    function.open(org.apache.flink.api.common.functions.DefaultOpenContext.INSTANCE);
  }

  public static org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger<
          org.apache.flink.table.data.RowData>
      miniBatchTrigger(org.apache.flink.configuration.ReadableConfig config) {
    return org.apache.flink.table.planner.plan.utils.MinibatchUtil.createMiniBatchTrigger(config);
  }

  public static void open(GroupAggFunction function, RuntimeContext context) throws Exception {
    function.setRuntimeContext(context);
    function.open(org.apache.flink.api.common.functions.DefaultOpenContext.INSTANCE);
  }
}
