package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;

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
    if (function instanceof org.apache.flink.table.functions.SpecializedFunction) return false;
    JoinRelType join = UnnestMatcher.joinType(node);
    return (join == JoinRelType.INNER || join == JoinRelType.LEFT)
        && JvmFunctionTypes.scalarRow(node.getInput().getRowType())
        && JvmFunctionTypes.scalarRow(node.getRowType())
        && ((RexCall) node.scan().getCall())
            .getOperands().stream().allMatch(arg -> JvmFunctionTypes.scalar(arg.getType()));
  }

  static String unsupportedReason(StreamPhysicalCorrelate node) {
    return "correlate: only non-specialized JVM table functions with INNER/LEFT and CHAR/VARCHAR,"
        + " numeric, boolean, binary, interval or temporal input, output and argument"
        + " types are verified";
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
