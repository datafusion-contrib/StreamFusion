package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;

final class JvmGroupAggregateMatcher {
  private JvmGroupAggregateMatcher() {}

  static boolean enabledAggregate(StreamPhysicalGroupAggregate node) {
    if (!Boolean.parseBoolean(
        System.getProperty("streamfusion.experimental.jvmGroupAggregates", "true"))) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (userFunction(call.getAggregation()) != null
          || call.getAggregation()
              == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT
          || tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation()))
        return true;
    }
    return false;
  }

  private static boolean integerCollectionAggregate(
      StreamPhysicalGroupAggregate node, org.apache.calcite.rel.core.AggregateCall call) {
    boolean collect =
        call.getAggregation()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT;
    if ((!collect && !tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation()))
        || call.getArgList().size() != 1) return false;
    var value = node.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
    return java.util.Set.of(
                org.apache.calcite.sql.type.SqlTypeName.INTEGER,
                org.apache.calcite.sql.type.SqlTypeName.BIGINT)
            .contains(value.getSqlTypeName())
        && call.getType().getSqlTypeName()
            == (collect
                ? org.apache.calcite.sql.type.SqlTypeName.MULTISET
                : org.apache.calcite.sql.type.SqlTypeName.ARRAY)
        && call.getType().getComponentType().getSqlTypeName() == value.getSqlTypeName();
  }

  private static AggregateFunction<?, ?> userFunction(SqlAggFunction function) {
    Object definition =
        function instanceof BridgingSqlAggFunction
            ? ((BridgingSqlAggFunction) function).getDefinition()
            : function.getClass() == AggSqlFunction.class
                ? ((AggSqlFunction) function).aggregateFunction()
                : null;
    return definition instanceof AggregateFunction ? (AggregateFunction<?, ?>) definition : null;
  }

  static boolean matches(StreamPhysicalGroupAggregate node) {
    var config = ShortcutUtils.unwrapTableConfig(node);
    if (config.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        || config.get(
            org.apache.flink.configuration.ConfigOptions.key("table.exec.async-state.enabled")
                .booleanType()
                .defaultValue(false))) return false;
    if (!JvmFunctionTypes.scalarRow(node.getInput().getRowType())) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (!JvmFunctionTypes.scalar(call.getType()) && !integerCollectionAggregate(node, call))
        return false;
      if (integerCollectionAggregate(node, call)
          && node.getRowType().getFieldList().stream()
              .anyMatch(field -> JsonStringIdentity.containsCharacter(field.getType())))
        return false;
      if (call.isApproximate() || !call.getCollation().getFieldCollations().isEmpty()) return false;
      var aggregation = call.getAggregation();
      if (aggregation instanceof AggSqlFunction && aggregation.getClass() != AggSqlFunction.class)
        return false;
      var function = userFunction(aggregation);
      if (function instanceof org.apache.flink.table.functions.python.PythonFunction
          || function instanceof SpecializedFunction) return false;
      if (aggregation instanceof BridgingSqlAggFunction
          && ((BridgingSqlAggFunction) aggregation).getDefinition() instanceof SpecializedFunction)
        return false;
    }
    return true;
  }

  static String unsupportedReason(StreamPhysicalGroupAggregate node) {
    return "GROUP BY: JVM aggregates require verified scalar fields or integer COLLECT/ARRAY_AGG"
               + " with non-character results and synchronous single-phase execution without"
               + " mini-batching, specialization, approximate or ordered calls";
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
