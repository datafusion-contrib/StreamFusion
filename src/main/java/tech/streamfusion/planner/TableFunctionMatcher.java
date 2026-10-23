package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
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
        && (scalarBoundary(node) || GeneratedUnnestMatcher.verifiedInput(node.getInput()))
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

  static String unsupportedReason(StreamPhysicalCorrelate node) {
    return "correlate: only non-specialized JVM table functions with INNER/LEFT and CHAR/VARCHAR,"
        + " numeric, boolean, binary, interval, temporal or recursively verified"
        + " ARRAY/MAP/ROW input, output and argument types with default external Java"
        + " conversions are verified";
  }

  static RelNode substitute(StreamPhysicalCorrelate node, PlanContext context) {
    return new StreamPhysicalNativeTableFunction(
        node.getCluster(),
        node.getTraitSet(),
        node.getInput(),
        node.getRowType(),
        (RexCall) node.scan().getCall(),
        node.condition().isDefined() ? node.condition().get() : null,
        UnnestMatcher.joinType(node) == JoinRelType.LEFT);
  }
}
