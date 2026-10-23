package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;

/** Admission for released Flink JVM table-function generation inside a columnar boundary. */
final class TableFunctionMatcher {
  private TableFunctionMatcher() {}

  static boolean isUserTableFunction(StreamPhysicalCorrelate node) {
    if (!(node.scan().getCall() instanceof RexCall)) return false;
    var operator = ((RexCall) node.scan().getCall()).getOperator();
    return operator instanceof BridgingSqlFunction
        && ((BridgingSqlFunction) operator).getDefinition() instanceof TableFunction;
  }

  static boolean matches(StreamPhysicalCorrelate node) {
    var function =
        ((BridgingSqlFunction) ((RexCall) node.scan().getCall()).getOperator()).getDefinition();
    if (function instanceof SpecializedFunction) return false;
    JoinRelType join = UnnestMatcher.joinType(node);
    return (join == JoinRelType.INNER || join == JoinRelType.LEFT)
        && verifiedConversions(node)
        && (scalarBoundary(node)
            || GeneratedUnnestMatcher.verifiedInput(node.getInput())
            || projectedSource(node.getInput()) != null)
        && JvmFunctionTypes.nestedRow(node.getInput().getRowType())
        && JvmFunctionTypes.nestedRow(node.getRowType())
        && ((RexCall) node.scan().getCall())
            .getOperands().stream().allMatch(arg -> JvmFunctionTypes.nested(arg.getType()));
  }

  private static boolean scalarBoundary(StreamPhysicalCorrelate node) {
    return JvmFunctionTypes.scalarRow(node.getInput().getRowType())
        && JvmFunctionTypes.scalarRow(node.getRowType())
        && ((RexCall) node.scan().getCall())
            .getOperands().stream().allMatch(arg -> JvmFunctionTypes.scalar(arg.getType()));
  }

  private static boolean verifiedConversions(StreamPhysicalCorrelate node) {
    RexCall call = (RexCall) node.scan().getCall();
    if (scalarBoundary(node)) return true;
    var function = (BridgingSqlFunction) call.getOperator();
    try {
      var context =
          new OperatorBindingCallContext(
              function.getDataTypeFactory(),
              function.getDefinition(),
              RexCallBinding.create(function.getTypeFactory(), call, null, Collections.emptyList()),
              call.getType());
      var inferred = TypeInferenceUtil.runTypeInference(function.getTypeInference(), context, null);
      return inferred.getExpectedArgumentTypes().stream()
              .allMatch(TableFunctionMatcher::defaultConversions)
          && defaultConversions(inferred.getOutputDataType());
    } catch (RuntimeException failure) {
      return false;
    }
  }

  private static boolean defaultConversions(DataType type) {
    return type.getConversionClass() == type.getLogicalType().getDefaultConversion()
        && type.getChildren().stream().allMatch(TableFunctionMatcher::defaultConversions);
  }

  private static final class ProjectedSource {
    final RelNode source;
    final org.apache.calcite.rel.type.RelDataType type;

    ProjectedSource(RelNode source, org.apache.calcite.rel.type.RelDataType type) {
      this.source = source;
      this.type = type;
    }
  }

  private static boolean copyStableProjectionType(org.apache.calcite.rel.type.RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return true;
      case ROW:
        return type.getFieldList().stream()
            .allMatch(field -> copyStableProjectionType(field.getType()));
      default:
        return false;
    }
  }

  private static ProjectedSource projectedSource(RelNode input) {
    if (input.getRowType().getFieldList().stream()
        .anyMatch(field -> !copyStableProjectionType(field.getType()))) return null;
    RelNode current = input;
    int[] mapping =
        java.util.stream.IntStream.range(0, input.getRowType().getFieldCount()).toArray();
    while (current instanceof StreamPhysicalNativeCalc) {
      var program = ((StreamPhysicalNativeCalc) current).sourceProgram();
      if (program == null || program.getCondition() != null) return null;
      int[] columns = new int[program.getProjectList().size()];
      for (int i = 0; i < columns.length; i++) {
        var expression = program.expandLocalRef(program.getProjectList().get(i));
        if (expression instanceof RexCall
            && expression.getKind() == org.apache.calcite.sql.SqlKind.AS)
          expression = ((RexCall) expression).getOperands().get(0);
        if (!(expression instanceof RexInputRef)) return null;
        columns[i] = ((RexInputRef) expression).getIndex();
      }
      mapping = java.util.Arrays.stream(mapping).map(index -> columns[index]).toArray();
      RelNode source = current.getInput(0);
      if (source instanceof StreamPhysicalRowDataToArrow) source = source.getInput(0);
      if (!source.getRowType().equals(program.getInputRowType())) return null;
      if (source instanceof StreamPhysicalNativeCalc) {
        current = source;
        continue;
      }
      if (source instanceof ColumnarOutput) return null;
      var type = input.getCluster().getTypeFactory().builder();
      var selected = new java.util.HashSet<Integer>();
      for (int i = 0; i < mapping.length; i++) {
        if (!selected.add(mapping[i])) return null;
        var field = source.getRowType().getFieldList().get(mapping[i]);
        if (!input.getRowType().getFieldList().get(i).getType().equals(field.getType()))
          return null;
        // Correlate reads ordinals and applies its declared output aliases. Entry pruning needs
        // source names so its owned copy selects the same fields across stacked projections.
        type.add(field.getName(), field.getType());
      }
      return new ProjectedSource(source, type.build());
    }
    return null;
  }

  static String unsupportedReason(StreamPhysicalCorrelate node) {
    return "correlate: only non-specialized JVM table functions with INNER/LEFT and CHAR/VARCHAR,"
        + " numeric, boolean, binary, interval, temporal or recursively verified"
        + " ARRAY/MAP/ROW input, output and argument types with default external Java"
        + " conversions and input representation are verified";
  }

  static RelNode substitute(StreamPhysicalCorrelate node, PlanContext context) {
    RelNode input = node.getInput();
    var projected = scalarBoundary(node) ? null : projectedSource(input);
    if (projected != null) {
      RelNode source = projected.source;
      boolean kinds =
          source instanceof StreamPhysicalRel
              && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) source);
      input =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, kinds, projected.type, true);
    }
    return new StreamPhysicalNativeTableFunction(
        node.getCluster(),
        node.getTraitSet(),
        input,
        node.getRowType(),
        (RexCall) node.scan().getCall(),
        node.condition().isDefined() ? node.condition().get() : null,
        UnnestMatcher.joinType(node) == JoinRelType.LEFT);
  }
}
