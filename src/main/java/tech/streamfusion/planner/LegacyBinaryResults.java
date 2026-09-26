package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/** Flink 1.18 SQL ENCODE declares fixed BINARY but produces variable-length byte arrays. */
final class LegacyBinaryResults {
  private LegacyBinaryResults() {}

  static boolean containsEncode(RexNode node) {
    if (!(node instanceof RexCall call)) return false;
    return call.getOperator().getName().equals("ENCODE")
            && call.getType().getSqlTypeName() == SqlTypeName.BINARY
        || call.getOperands().stream().anyMatch(LegacyBinaryResults::containsEncode);
  }

  static boolean variableResult(RexNode node) {
    return node.getType().getSqlTypeName() == SqlTypeName.BINARY && containsEncode(node);
  }

  static boolean projects(RelNode node) {
    if (node instanceof StreamPhysicalCalc calc) {
      var program = calc.getProgram();
      return program.getProjectList().stream()
          .anyMatch(ref -> variableResult(program.expandLocalRef(ref)));
    }
    return false;
  }

  static boolean crossesBoundary(RelNode node, boolean finalOutput) {
    for (RelNode input : node.getInputs()) {
      if (projects(input) && !(node instanceof StreamPhysicalSink)) return true;
      if (crossesBoundary(input, true)) return true;
    }
    return projects(node) && !finalOutput;
  }

  static boolean reachesSink(RelNode node) {
    return projects(node) || node.getInputs().stream().anyMatch(LegacyBinaryResults::reachesSink);
  }
}
