package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;

final class JvmGroupAggregateMatcher {
  private JvmGroupAggregateMatcher() {}

  static boolean enabledUserAggregate(StreamPhysicalGroupAggregate node) {
    if (!Boolean.parseBoolean(
        System.getProperty("streamfusion.experimental.jvmGroupAggregates", "true"))) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var agg = node.aggCalls().apply(i).getAggregation();
      if (agg instanceof BridgingSqlAggFunction
          && ((BridgingSqlAggFunction) agg).getDefinition() instanceof AggregateFunction)
        return true;
    }
    return false;
  }

  static boolean matches(StreamPhysicalGroupAggregate node) {
    var config = ShortcutUtils.unwrapTableConfig(node);
    if (config.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        || config.get(
            org.apache.flink.configuration.ConfigOptions.key("table.exec.async-state.enabled")
                .booleanType()
                .defaultValue(false))) return false;
    if (!JvmFunctionTypes.scalarRow(node.getInput().getRowType())
        || !JvmFunctionTypes.scalarRow(node.getRowType())) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (call.isApproximate() || !call.getCollation().getFieldCollations().isEmpty()) return false;
      if (call.getAggregation() instanceof BridgingSqlAggFunction
          && ((BridgingSqlAggFunction) call.getAggregation()).getDefinition()
              instanceof SpecializedFunction) return false;
    }
    return true;
  }

  static String unsupportedReason(StreamPhysicalGroupAggregate node) {
    return "GROUP BY: JVM aggregates require verified scalar fields and synchronous single-phase"
        + " execution without mini-batching, specialization, approximate"
        + " or ordered calls";
  }

  static RelNode substitute(StreamPhysicalGroupAggregate node, PlanContext context) {
    int[] keys = GroupAggregateMatcher.keyColumns(node);
    return new StreamPhysicalNativeJvmGroup(
        node.getCluster(),
        node.getTraitSet(),
        context.columnarInput(node.getInput(), keys),
        node.getRowType(),
        keys,
        node.aggCalls(),
        AggregateUtil.deriveAggCallNeedRetractions(node, keys.length, node.aggCalls()),
        !ChangelogPlanUtils.inputInsertOnly(node),
        ChangelogPlanUtils.generateUpdateBefore(node),
        tech.streamfusion.compat.FlinkCompat.singleStateTtl(node));
  }
}
