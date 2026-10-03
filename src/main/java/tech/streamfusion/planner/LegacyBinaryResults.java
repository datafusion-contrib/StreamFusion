package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/** Legacy binary casts and Flink 1.18 ENCODE can return bytes beyond their declared fixed width. */
final class LegacyBinaryResults {
  private LegacyBinaryResults() {}

  static boolean containsEncode(RexNode node) {
    if (!(node instanceof RexCall call)) return false;
    return call.getOperator().getName().equals("ENCODE")
            && call.getType().getSqlTypeName() == SqlTypeName.BINARY
        || call.getOperands().stream().anyMatch(LegacyBinaryResults::containsEncode);
  }

  private static boolean containsLegacyCast(RexNode node) {
    if (!(node instanceof RexCall call)) return false;
    return call.getType().getSqlTypeName() == SqlTypeName.BINARY
            && (call.getKind() == org.apache.calcite.sql.SqlKind.CAST
                || call.getOperator()
                    == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST)
        || call.getOperands().stream().anyMatch(LegacyBinaryResults::containsLegacyCast);
  }

  static boolean variableResult(RexNode node, boolean legacyCast) {
    return node.getType().getSqlTypeName() == SqlTypeName.BINARY
        && (containsEncode(node) || legacyCast && containsLegacyCast(node));
  }

  static boolean projects(RelNode node) {
    if (node instanceof StreamPhysicalCalc calc) {
      var program = calc.getProgram();
      boolean legacyCast = org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(calc)
          .get(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
          .isEnabled();
      return program.getProjectList().stream()
          .anyMatch(ref -> variableResult(program.expandLocalRef(ref), legacyCast));
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
