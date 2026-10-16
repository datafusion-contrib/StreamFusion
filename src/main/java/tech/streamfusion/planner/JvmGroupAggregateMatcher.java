package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
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
    if (!Boolean.getBoolean("streamfusion.experimental.jvmGroupAggregates")) return false;
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
    var retention = tech.streamfusion.compat.FlinkCompat.singleStateTtl(node);
    if (config.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        || config.getIdleStateRetention().toMillis() > 0
        || retention != null && retention > 0
        || config.get(
            org.apache.flink.configuration.ConfigOptions.key("table.exec.async-state.enabled")
                .booleanType()
                .defaultValue(false))) return false;
    if (!basicRow(node.getInput().getRowType()) || !basicRow(node.getRowType())) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (call.isDistinct()
          || call.isApproximate()
          || call.filterArg >= 0
          || !call.getCollation().getFieldCollations().isEmpty()) return false;
      if (call.getAggregation() instanceof BridgingSqlAggFunction
          && ((BridgingSqlAggFunction) call.getAggregation()).getDefinition()
              instanceof SpecializedFunction) return false;
    }
    return true;
  }

  private static boolean basicRow(RelDataType row) {
    return row.getFieldList().stream()
        .allMatch(
            field -> {
              switch (field.getType().getSqlTypeName()) {
                case VARCHAR:
                case INTEGER:
                case BIGINT:
                  return true;
                default:
                  return false;
              }
            });
  }

  static String unsupportedReason(StreamPhysicalGroupAggregate node) {
    return "GROUP BY: JVM aggregates require verified columnar input, keyed state and performance"
               + " admission";
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
        ChangelogPlanUtils.generateUpdateBefore(node));
  }
}
