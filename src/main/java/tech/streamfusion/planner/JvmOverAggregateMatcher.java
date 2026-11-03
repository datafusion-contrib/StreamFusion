package tech.streamfusion.planner;

import java.util.Set;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;

final class JvmOverAggregateMatcher {
  private static final Set<SqlKind> COMPANIONS =
      Set.of(
          SqlKind.SUM,
          SqlKind.SUM0,
          SqlKind.MIN,
          SqlKind.MAX,
          SqlKind.COUNT,
          SqlKind.AVG,
          SqlKind.FIRST_VALUE,
          SqlKind.LAST_VALUE,
          SqlKind.ROW_NUMBER,
          SqlKind.RANK,
          SqlKind.DENSE_RANK);

  private JvmOverAggregateMatcher() {}

  static boolean enabled(StreamPhysicalOverAggregate node) {
    if (!NativeConfig.operatorEnabled("over")) return false;
    return node.logicWindow().groups.stream()
        .flatMap(group -> group.aggCalls.stream())
        .anyMatch(call -> userFunction(call.getOperator()) != null);
  }

  private static AggregateFunction<?, ?> userFunction(SqlOperator operator) {
    return operator instanceof SqlAggFunction
        ? JvmGroupAggregateMatcher.userFunction((SqlAggFunction) operator)
        : null;
  }

  static boolean matches(StreamPhysicalOverAggregate node) {
    return unsupportedReason(node) == null;
  }

  static String unsupportedReason(StreamPhysicalOverAggregate node) {
    var config = ShortcutUtils.unwrapTableConfig(node);
    if (config.get(
        ConfigOptions.key("table.exec.async-state.enabled").booleanType().defaultValue(false)))
      return "OVER: asynchronous JVM state is not verified";
    if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) node.getInput()))
      return "OVER: JVM aggregates require insert-only input";
    if (!JvmFunctionTypes.scalarRow(node.getInput().getRowType())
        || !JvmFunctionTypes.scalarRow(node.getRowType()))
      return "OVER: JVM aggregates require verified scalar input, partition and result fields";
    Window window = node.logicWindow();
    if (window.groups.size() != 1) return "OVER: JVM aggregates require one window group";
    var group = window.groups.get(0);
    var orders = group.orderKeys.getFieldCollations();
    if (orders.size() != 1 || orders.get(0).getDirection() != RelFieldCollation.Direction.ASCENDING)
      return "OVER: JVM aggregates require one ascending time order";
    var orderType =
        node.getInput().getRowType().getFieldList().get(orders.get(0).getFieldIndex()).getType();
    boolean rowtime = FlinkTypeFactory.isRowtimeIndicatorType(orderType);
    boolean proctime = FlinkTypeFactory.isProctimeIndicatorType(orderType);
    if (!rowtime && !proctime)
      return "OVER: JVM aggregates require an event-time or processing-time order";
    if (!group.upperBound.isCurrentRow() || !group.lowerBound.isPreceding())
      return "OVER: JVM aggregates require PRECEDING through CURRENT ROW";
    if (proctime && !group.isRows && !group.lowerBound.isUnbounded())
      return "OVER: JVM processing-time bounded RANGE is outside the verified frame boundary";
    for (var call : group.aggCalls) {
      var aggregation = call.getOperator();
      var function = userFunction(aggregation);
      if (function == null && !COMPANIONS.contains(aggregation.getKind()))
        return "OVER: unverified companion aggregate";
      if (aggregation instanceof AggSqlFunction && aggregation.getClass() != AggSqlFunction.class
          || function instanceof SpecializedFunction
          || function instanceof PythonFunction
          || aggregation instanceof BridgingSqlAggFunction
              && ((BridgingSqlAggFunction) aggregation).getDefinition()
                  instanceof SpecializedFunction)
        return "OVER: specialized, Python and custom legacy JVM wrappers are not verified";
      if (!JvmFunctionTypes.scalar(call.getType()))
        return "OVER: JVM aggregates require verified scalar results";
    }
    return null;
  }

  static RelNode substitute(StreamPhysicalOverAggregate node, PlanContext context) {
    int[] keys = node.logicWindow().groups.get(0).keys.toArray();
    return new StreamPhysicalNativeJvmOver(
        node.getCluster(),
        node.getTraitSet(),
        context.columnarInput(node.getInput(), keys, false, true),
        node.getRowType(),
        node.translateToExecNode(),
        !OverAggregateMatcher.isProctime(node));
  }
}
