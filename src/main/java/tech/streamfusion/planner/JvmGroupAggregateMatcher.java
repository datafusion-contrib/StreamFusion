package tech.streamfusion.planner;

import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;

final class JvmGroupAggregateMatcher {
  private static final Set<SqlTypeName> COLLECTION_ELEMENTS =
      Set.of(
          SqlTypeName.BINARY,
          SqlTypeName.VARBINARY,
          SqlTypeName.CHAR,
          SqlTypeName.VARCHAR,
          SqlTypeName.BOOLEAN,
          SqlTypeName.TINYINT,
          SqlTypeName.SMALLINT,
          SqlTypeName.INTEGER,
          SqlTypeName.BIGINT,
          SqlTypeName.FLOAT,
          SqlTypeName.DOUBLE,
          SqlTypeName.DECIMAL,
          SqlTypeName.DATE,
          SqlTypeName.TIME,
          SqlTypeName.TIMESTAMP,
          SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

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

  static boolean verifiedCollectionAggregate(
      StreamPhysicalGroupAggregate node, org.apache.calcite.rel.core.AggregateCall call) {
    boolean collect =
        call.getAggregation()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT;
    if ((!collect && !tech.streamfusion.compat.FlinkCompat.isBuiltinArrayAgg(call.getAggregation()))
        || call.getArgList().size() != 1) return false;
    var value = node.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
    return collectionElement(value)
        && call.getType().getSqlTypeName()
            == (collect
                ? org.apache.calcite.sql.type.SqlTypeName.MULTISET
                : org.apache.calcite.sql.type.SqlTypeName.ARRAY)
        && call.getType().getComponentType().getSqlTypeName() == value.getSqlTypeName()
        && (value.getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.DECIMAL
            || value.getPrecision() == call.getType().getComponentType().getPrecision()
                && value.getScale() == call.getType().getComponentType().getScale());
  }

  private static boolean collectionElement(org.apache.calcite.rel.type.RelDataType type) {
    var family = type.getSqlTypeName().getFamily();
    return RexExpression.collectionAggregateElement(type)
        || COLLECTION_ELEMENTS.contains(type.getSqlTypeName())
        || family == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_YEAR_MONTH
        || family == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME;
  }

  static boolean unverifiedCollectionInputMaterialization(RelNode node) {
    if (node instanceof StreamPhysicalGroupAggregate) {
      var aggregate = (StreamPhysicalGroupAggregate) node;
      boolean collection = false;
      for (int i = 0; i < aggregate.aggCalls().size(); i++)
        collection |= GroupAggregateMatcher.collectionKind(aggregate.aggCalls().apply(i)) >= 0;
      if (collection && materializesInputHints(aggregate.getInput())) return true;
    }
    return node.getInputs().stream()
        .anyMatch(JvmGroupAggregateMatcher::unverifiedCollectionInputMaterialization);
  }

  // A released Calc can expose a NULL source row's hidden primitive payload under these
  // hints. Arrow validity retains the NULL, not that source-specific unchecked getter value.
  private static boolean materializesInputHints(RelNode node) {
    if (node instanceof org.apache.calcite.rel.core.Calc) {
      var calc = (org.apache.calcite.rel.core.Calc) node;
      var program = calc.getProgram();
      if (program.getCondition() != null
          && readsInputHint(program.expandLocalRef(program.getCondition()))) return true;
      boolean identity =
          program.getProjectList().size() == calc.getInput().getRowType().getFieldCount();
      for (int i = 0; identity && i < program.getProjectList().size(); i++) {
        var projection = program.expandLocalRef(program.getProjectList().get(i));
        identity =
            projection instanceof org.apache.calcite.rex.RexInputRef
                && ((org.apache.calcite.rex.RexInputRef) projection).getIndex() == i;
      }
      if (!identity
          && program.getProjectList().stream()
              .map(program::expandLocalRef)
              .anyMatch(JvmGroupAggregateMatcher::readsInputHint)) return true;
    }
    return node.getInputs().stream().anyMatch(JvmGroupAggregateMatcher::materializesInputHints);
  }

  private static boolean readsInputHint(org.apache.calcite.rex.RexNode expression) {
    boolean[] found = {false};
    expression.accept(
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(org.apache.calcite.rex.RexInputRef input) {
            var type = input.getType();
            if (!type.isNullable() && collectionElement(type)) found[0] = true;
            return null;
          }
        });
    return found[0];
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

  static boolean requiresCollectionInputs(org.apache.calcite.rel.type.RelDataType rowType) {
    return rowType.getFieldList().stream()
        .anyMatch(field -> RexExpression.collectionAggregateElement(field.getType()));
  }

  static boolean hasNestedCollectionElements(StreamPhysicalGroupAggregate node) {
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (verifiedCollectionAggregate(node, call)
          && RexExpression.collectionAggregateElement(call.getType().getComponentType()))
        return true;
    }
    return false;
  }

  static boolean matches(StreamPhysicalGroupAggregate node) {
    for (int key : GroupAggregateMatcher.keyColumns(node))
      if (!JvmFunctionTypes.scalar(node.getInput().getRowType().getFieldList().get(key).getType()))
        return false;
    boolean nestedCollection = hasNestedCollectionElements(node);
    boolean input =
        JvmFunctionTypes.scalarRow(node.getInput().getRowType())
            || nestedCollection
                && node.getInput().getRowType().getFieldList().stream()
                    .allMatch(
                        field ->
                            JvmFunctionTypes.scalar(field.getType())
                                || RexExpression.collectionAggregateElement(field.getType()));
    return input && matchesContext(node);
  }

  static boolean matchesContext(StreamPhysicalGroupAggregate node) {
    var config = ShortcutUtils.unwrapTableConfig(node);
    if (config.get(
        org.apache.flink.configuration.ConfigOptions.key("table.exec.async-state.enabled")
            .booleanType()
            .defaultValue(false))) return false;
    if (config.get(
            org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        && (!JvmFunctionTypes.scalarRow(node.getInput().getRowType())
            || !JvmFunctionTypes.scalarRow(node.getRowType()))) return false;
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (!JvmFunctionTypes.scalar(call.getType()) && !verifiedCollectionAggregate(node, call))
        return false;
      if (verifiedCollectionAggregate(node, call) && unverifiedCharacterResult(node)) return false;
      if (call.isApproximate() || !call.getCollation().getFieldCollations().isEmpty()) return false;
      var aggregation = call.getAggregation();
      if (aggregation instanceof AggSqlFunction && aggregation.getClass() != AggSqlFunction.class)
        return false;
      var function = userFunction(aggregation);
      if (function != null
          && call.getArgList().stream()
              .anyMatch(
                  index ->
                      !JvmFunctionTypes.scalar(
                          node.getInput().getRowType().getFieldList().get(index).getType())))
        return false;
      if (function instanceof org.apache.flink.table.functions.python.PythonFunction
          || function instanceof SpecializedFunction) return false;
      if (aggregation instanceof BridgingSqlAggFunction
          && ((BridgingSqlAggFunction) aggregation).getDefinition() instanceof SpecializedFunction)
        return false;
    }
    return true;
  }

  private static boolean unverifiedCharacterResult(StreamPhysicalGroupAggregate node) {
    for (int i = 0; i < node.aggCalls().size(); i++) {
      var call = node.aggCalls().apply(i);
      if (JsonStringIdentity.containsCharacter(call.getType())
          && !verifiedCollectionAggregate(node, call)) return true;
    }
    return false;
  }

  static String unsupportedReason(StreamPhysicalGroupAggregate node) {
    return "GROUP BY: JVM aggregates require verified scalar fields or verified COLLECT/ARRAY_AGG"
               + " with verified collection elements, non-character scalar coresults and"
               + " synchronous scalar single-phase mini-batching or ordinary execution without"
               + " specialization, approximate or ordered calls";
  }

  static RelNode substitute(StreamPhysicalGroupAggregate node, PlanContext context) {
    int[] keys = GroupAggregateMatcher.keyColumns(node);
    return new StreamPhysicalNativeJvmGroup(
        node.getCluster(),
        node.getTraitSet(),
        context.columnarInput(
            node.getInput(),
            keys,
            false,
            ShortcutUtils.unwrapTableConfig(node)
                .get(
                    org.apache.flink.table.api.config.ExecutionConfigOptions
                        .TABLE_EXEC_MINIBATCH_ENABLED)),
        node.getRowType(),
        keys,
        node.aggCalls(),
        AggregateUtil.deriveAggCallNeedRetractions(node, keys.length, node.aggCalls()),
        !ChangelogPlanUtils.inputInsertOnly(node),
        ChangelogPlanUtils.generateUpdateBefore(node),
        tech.streamfusion.compat.FlinkCompat.singleStateTtl(node));
  }
}
