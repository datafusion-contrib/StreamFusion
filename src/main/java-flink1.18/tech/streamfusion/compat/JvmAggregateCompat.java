package tech.streamfusion.compat;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;

/** Uses the released rich-function opening contract for this Flink line. */
public final class JvmAggregateCompat {
  private JvmAggregateCompat() {}

  public static void open(GroupAggFunction function, RuntimeContext context) throws Exception {
    function.setRuntimeContext(context);
    function.open(new org.apache.flink.configuration.Configuration());
  }
}
