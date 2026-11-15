package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import scala.collection.Seq;
import tech.streamfusion.operator.JvmAggregatePartialCodec;
import tech.streamfusion.planner.JvmFunctionTypes;

/** The released planner has already established merge eligibility for these physical phases. */
final class JvmTwoPhaseAggregateMatcher {
  private JvmTwoPhaseAggregateMatcher() {}

  static boolean enabled(Seq<AggregateCall> calls) {
    if (!Boolean.parseBoolean(
        System.getProperty("streamfusion.experimental.jvmGroupAggregates", "true"))) return false;
    for (int i = 0; i < calls.size(); i++)
      if (JvmGroupAggregateMatcher.userFunction(calls.apply(i).getAggregation()) != null)
        return true;
    return false;
  }

  private static boolean matches(RelNode node, RelDataType input, Seq<AggregateCall> calls) {
    var config = ShortcutUtils.unwrapTableConfig(node);
    if (!JvmFunctionTypes.scalarRow(input)
        || !config.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        || config.get(
            org.apache.flink.configuration.ConfigOptions.key("table.exec.async-state.enabled")
                .booleanType()
                .defaultValue(false))) return false;
    for (int i = 0; i < calls.size(); i++) {
      var call = calls.apply(i);
      var aggregation = call.getAggregation();
      var function = JvmGroupAggregateMatcher.userFunction(aggregation);
      if (!JvmFunctionTypes.scalar(call.getType())
          || call.isApproximate()
          || !call.getCollation().getFieldCollations().isEmpty()
          || aggregation instanceof AggSqlFunction && aggregation.getClass() != AggSqlFunction.class
          || function instanceof SpecializedFunction
          || function instanceof org.apache.flink.table.functions.python.PythonFunction
          || aggregation instanceof BridgingSqlAggFunction
              && ((BridgingSqlAggFunction) aggregation).getDefinition()
                  instanceof SpecializedFunction) return false;
    }
    return true;
  }

  static boolean matches(StreamPhysicalLocalGroupAggregate node) {
    return matches(node, node.getInput().getRowType(), node.aggCalls());
  }

  static boolean matches(StreamPhysicalGlobalGroupAggregate node) {
    return JvmFunctionTypes.scalarRow(node.getRowType())
        && matches(node, node.localAggInputRowType(), node.aggCalls());
  }

  static String unsupportedReason(RelNode node) {
    return "GROUP BY: two-phase JVM aggregates require synchronous mini-batches, verified scalar"
        + " input/output fields and non-specialized JVM functions without approximate or"
        + " ordered calls";
  }

  static RelNode substitute(StreamPhysicalLocalGroupAggregate node, PlanContext context) {
    var global = context.globalGroupForLocal(node);
    var partial = FlinkTypeFactory.toLogicalRowType(node.getRowType());
    var factory = (FlinkTypeFactory) node.getCluster().getTypeFactory();
    var builder = factory.builder();
    for (var field : JvmAggregatePartialCodec.transportType(partial).getFields())
      builder.add(field.getName(), factory.createFieldTypeFromLogicalType(field.getType()));
    var keys = LocalGroupAggregateMatcher.keyColumns(node);
    return new StreamPhysicalNativeJvmTwoPhaseGroup(
        node.getCluster(),
        node.getTraitSet(),
        node.getInput(),
        builder.build(),
        FlinkTypeFactory.toLogicalRowType(node.getInput().getRowType()),
        partial,
        keys,
        global.needRetraction(),
        false,
        null,
        global.localAggInfoList(),
        null);
  }

  static RelNode substitute(StreamPhysicalGlobalGroupAggregate node, PlanContext context) {
    var keys = node.grouping();
    // Reconstruct the partial schema from the released local accumulator types, before transport.
    var fields = new java.util.ArrayList<org.apache.flink.table.types.logical.RowType.RowField>();
    for (int key : keys)
      fields.add(
          new org.apache.flink.table.types.logical.RowType.RowField(
              node.getInput().getRowType().getFieldNames().get(key),
              FlinkTypeFactory.toLogicalType(
                  node.getInput().getRowType().getFieldList().get(key).getType())));
    var partialNames = node.getInput().getRowType().getFieldNames();
    var accTypes = node.localAggInfoList().getAccTypes();
    for (int i = 0; i < accTypes.length; i++)
      fields.add(
          new org.apache.flink.table.types.logical.RowType.RowField(
              partialNames.get(keys.length + i),
              org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter
                  .fromDataTypeToLogicalType(accTypes[i])));
    var partial = new org.apache.flink.table.types.logical.RowType(fields);
    return new StreamPhysicalNativeJvmTwoPhaseGroup(
        node.getCluster(),
        node.getTraitSet(),
        context.columnarInput(node.getInput(), keys, false, true),
        node.getRowType(),
        FlinkTypeFactory.toLogicalRowType(node.localAggInputRowType()),
        partial,
        keys,
        node.needRetraction(),
        ChangelogPlanUtils.generateUpdateBefore(node),
        tech.streamfusion.compat.FlinkCompat.singleStateTtl(node),
        node.localAggInfoList(),
        node.globalAggInfoList());
  }
}
